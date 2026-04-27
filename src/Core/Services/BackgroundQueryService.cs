using System.Collections.Concurrent;
using System.Threading.Channels;
using NexusDataSpace.Core.Models;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace NexusDataSpace.Core.Services;

public sealed class BackgroundQueryService
{
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly OperationalMetricsService _metrics;
    private readonly Channel<string> _queue;
    private readonly ConcurrentDictionary<string, QueryJobState> _jobs = new(StringComparer.Ordinal);

    public BackgroundQueryService(
        IServiceScopeFactory scopeFactory,
        IConfiguration configuration,
        OperationalMetricsService metrics)
    {
        _scopeFactory = scopeFactory;
        _metrics = metrics;
        _queue = Channel.CreateBounded<string>(new BoundedChannelOptions(ReadBoundedInt(configuration, "Load:BackgroundQueryQueueCapacity", 100, 1, 10_000))
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = false,
            SingleWriter = false
        });

        var workers = ReadBoundedInt(configuration, "Load:BackgroundQueryWorkers", 2, 1, 32);
        for (var i = 0; i < workers; i++)
            _ = Task.Run(RunWorkerAsync);
    }

    public async Task<QueryJobSnapshot> EnqueueAsync(
        QueryJobSubmitRequest request,
        int companyId,
        int? userId,
        string username,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(request.Text))
            throw new InvalidOperationException("Query text is empty.");

        var job = new QueryJobState(
            Guid.NewGuid().ToString("N"),
            NormalizeCompanyId(companyId),
            userId,
            username,
            request.Text.Trim(),
            request.History,
            request.PreviousIntent);
        if (!_jobs.TryAdd(job.Id, job))
            throw new InvalidOperationException("Не удалось создать фоновый запрос.");

        job.SetQueuedScope(_metrics.EnterBackgroundQueue());
        await _queue.Writer.WriteAsync(job.Id, cancellationToken);
        return job.ToSnapshot();
    }

    public QueryJobSnapshot? Get(string id, int companyId, string username, bool isAdmin)
    {
        if (!_jobs.TryGetValue(id, out var job))
            return null;
        if (job.CompanyId != NormalizeCompanyId(companyId))
            return null;
        if (!isAdmin && !string.Equals(job.Username, username, StringComparison.OrdinalIgnoreCase))
            return null;

        return job.ToSnapshot();
    }

    public bool Cancel(string id, int companyId, string username, bool isAdmin)
    {
        if (!_jobs.TryGetValue(id, out var job))
            return false;
        if (job.CompanyId != NormalizeCompanyId(companyId))
            return false;
        if (!isAdmin && !string.Equals(job.Username, username, StringComparison.OrdinalIgnoreCase))
            return false;

        job.Cancel();
        return true;
    }

    private async Task RunWorkerAsync()
    {
        await foreach (var jobId in _queue.Reader.ReadAllAsync())
        {
            if (!_jobs.TryGetValue(jobId, out var job))
                continue;
            if (job.IsCancellationRequested)
            {
                job.MarkCanceled("Запрос отменён до запуска.");
                continue;
            }

            using var activeJobScope = _metrics.EnterBackgroundJob();
            job.LeaveQueue();
            job.MarkRunning();
            try
            {
                using var scope = _scopeFactory.CreateScope();
                var tenantContext = scope.ServiceProvider.GetRequiredService<TenantContext>();
                var engine = scope.ServiceProvider.GetRequiredService<NlSqlEngine>();
                var audit = scope.ServiceProvider.GetRequiredService<AuditLogService>();
                using var tenantScope = tenantContext.Use(job.CompanyId);
                var result = await engine.RunAsync(
                    job.Text,
                    job.History,
                    job.PreviousIntent,
                    job.CancellationToken,
                    job.CompanyId,
                    job.Username);
                result.UserQuery = job.Text;
                job.MarkCompleted(result);
                audit.Record(job.CompanyId, job.UserId, job.Username, "query.background.run", "query_job", job.Id, string.IsNullOrWhiteSpace(result.Error), job.Text);
            }
            catch (OperationCanceledException)
            {
                job.MarkCanceled("Запрос отменён пользователем.");
            }
            catch (Exception exception)
            {
                job.MarkFailed(exception.Message);
            }
        }
    }

    private static int NormalizeCompanyId(int companyId) =>
        companyId <= 0 ? CompanyDefaults.DefaultCompanyId : companyId;

    private static int ReadBoundedInt(IConfiguration configuration, string key, int fallback, int min, int max) =>
        int.TryParse(configuration[key], out var value) ? Math.Clamp(value, min, max) : fallback;

    private sealed class QueryJobState
    {
        private readonly object _sync = new();
        private readonly CancellationTokenSource _cts = new();
        private IDisposable? _queuedScope;

        public QueryJobState(
            string id,
            int companyId,
            int? userId,
            string username,
            string text,
            IReadOnlyList<ChatTurn>? history,
            QueryIntent? previousIntent)
        {
            Id = id;
            CompanyId = companyId;
            UserId = userId;
            Username = username;
            Text = text;
            History = history;
            PreviousIntent = previousIntent;
        }

        public string Id { get; }
        public int CompanyId { get; }
        public int? UserId { get; }
        public string Username { get; }
        public string Text { get; }
        public IReadOnlyList<ChatTurn>? History { get; }
        public QueryIntent? PreviousIntent { get; }
        public DateTimeOffset CreatedAt { get; } = DateTimeOffset.UtcNow;
        public DateTimeOffset? StartedAt { get; private set; }
        public DateTimeOffset? CompletedAt { get; private set; }
        public string Status { get; private set; } = QueryJobStatuses.Queued;
        public PipelineResult? Result { get; private set; }
        public string? Error { get; private set; }
        public CancellationToken CancellationToken => _cts.Token;
        public bool IsCancellationRequested => _cts.IsCancellationRequested;

        public void Cancel()
        {
            if (!_cts.IsCancellationRequested)
                _cts.Cancel();
        }

        public void SetQueuedScope(IDisposable queuedScope)
        {
            lock (_sync)
            {
                _queuedScope = queuedScope;
            }
        }

        public void LeaveQueue()
        {
            IDisposable? queuedScope;
            lock (_sync)
            {
                queuedScope = _queuedScope;
                _queuedScope = null;
            }

            queuedScope?.Dispose();
        }

        public void MarkRunning()
        {
            lock (_sync)
            {
                if (_cts.IsCancellationRequested)
                {
                    MarkCanceled("Запрос отменён до запуска.");
                    return;
                }

                Status = QueryJobStatuses.Running;
                StartedAt = DateTimeOffset.UtcNow;
            }
        }

        public void MarkCompleted(PipelineResult result)
        {
            lock (_sync)
            {
                Result = result;
                Error = result.Error;
                Status = string.IsNullOrWhiteSpace(result.Error) ? QueryJobStatuses.Succeeded : QueryJobStatuses.Failed;
                CompletedAt = DateTimeOffset.UtcNow;
            }
        }

        public void MarkCanceled(string message)
        {
            lock (_sync)
            {
                Status = QueryJobStatuses.Canceled;
                Error = message;
                CompletedAt = DateTimeOffset.UtcNow;
            }
        }

        public void MarkFailed(string message)
        {
            lock (_sync)
            {
                Status = QueryJobStatuses.Failed;
                Error = message;
                CompletedAt = DateTimeOffset.UtcNow;
            }
        }

        public QueryJobSnapshot ToSnapshot()
        {
            lock (_sync)
            {
                return new QueryJobSnapshot(Id, Status, Text, CreatedAt, StartedAt, CompletedAt, Result, Error);
            }
        }
    }
}
