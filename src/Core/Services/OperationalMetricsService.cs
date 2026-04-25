using DriveeDataSpace.Core.Models;

namespace DriveeDataSpace.Core.Services;

public sealed record OperationalMetricsSnapshot(
    DateTimeOffset StartedAt,
    DateTimeOffset CapturedAt,
    int RequestsLastHour,
    int SuccessfulRequestsLastHour,
    int FailedRequestsLastHour,
    int RateLimitedLastHour,
    int ActiveQueryUsersLastHour,
    int LlmCallsLastHour,
    int LlmFailuresLastHour,
    int SqlExecutionsLastHour,
    int SqlFailuresLastHour,
    int IntentCacheHitsLastHour,
    int ResultCacheHitsLastHour,
    int ActiveRequests,
    int QueuedLlmRequests,
    int ActiveLlmRequests,
    int QueuedSqlQueries,
    int ActiveSqlQueries,
    int QueuedBackgroundJobs,
    int ActiveBackgroundJobs,
    double AverageLlmMs,
    double AverageSqlMs,
    double AverageTotalRequestMs,
    IReadOnlyList<OperationalMetricBucket> Buckets);

public sealed record OperationalMetricBucket(
    DateTimeOffset From,
    DateTimeOffset To,
    int Requests,
    int Errors,
    int RateLimited);

public sealed class OperationalMetricsService
{
    private const int MaxEvents = 20_000;
    private static readonly TimeSpan Retention = TimeSpan.FromHours(2);
    private static readonly TimeSpan SnapshotWindow = TimeSpan.FromHours(1);
    private static readonly TimeSpan BucketSize = TimeSpan.FromMinutes(5);

    private readonly object _sync = new();
    private readonly Queue<OperationalMetricEvent> _events = new();
    private int _activeRequests;
    private int _queuedLlmRequests;
    private int _activeLlmRequests;
    private int _queuedSqlQueries;
    private int _activeSqlQueries;
    private int _queuedBackgroundJobs;
    private int _activeBackgroundJobs;

    public DateTimeOffset StartedAt { get; } = DateTimeOffset.UtcNow;

    public IDisposable EnterRequest()
    {
        Interlocked.Increment(ref _activeRequests);
        return new CounterScope(() => Interlocked.Decrement(ref _activeRequests));
    }

    public QueueCounterScope EnterLlmQueue()
    {
        Interlocked.Increment(ref _queuedLlmRequests);
        return new QueueCounterScope(
            () => Interlocked.Decrement(ref _queuedLlmRequests),
            () => Interlocked.Increment(ref _activeLlmRequests),
            () => Interlocked.Decrement(ref _activeLlmRequests));
    }

    public QueueCounterScope EnterSqlQueue()
    {
        Interlocked.Increment(ref _queuedSqlQueries);
        return new QueueCounterScope(
            () => Interlocked.Decrement(ref _queuedSqlQueries),
            () => Interlocked.Increment(ref _activeSqlQueries),
            () => Interlocked.Decrement(ref _activeSqlQueries));
    }

    public IDisposable EnterBackgroundQueue()
    {
        Interlocked.Increment(ref _queuedBackgroundJobs);
        return new CounterScope(() => Interlocked.Decrement(ref _queuedBackgroundJobs));
    }

    public IDisposable EnterBackgroundJob()
    {
        Interlocked.Increment(ref _activeBackgroundJobs);
        return new CounterScope(() => Interlocked.Decrement(ref _activeBackgroundJobs));
    }

    public void RecordQuery(int companyId, string? userKey, bool success, long durationMs) =>
        Add(new OperationalMetricEvent(
            DateTimeOffset.UtcNow,
            NormalizeCompanyId(companyId),
            NormalizeUser(userKey),
            MetricKinds.Query,
            success,
            Math.Max(0, durationMs),
            null));

    public void RecordRateLimited(int companyId, string? userKey) =>
        Add(new OperationalMetricEvent(
            DateTimeOffset.UtcNow,
            NormalizeCompanyId(companyId),
            NormalizeUser(userKey),
            MetricKinds.RateLimited,
            false,
            0,
            null));

    public void RecordLlmCall(int companyId, string provider, bool success, long durationMs) =>
        Add(new OperationalMetricEvent(
            DateTimeOffset.UtcNow,
            NormalizeCompanyId(companyId),
            null,
            MetricKinds.Llm,
            success,
            Math.Max(0, durationMs),
            provider));

    public void RecordSqlExecution(int companyId, bool success, long durationMs) =>
        Add(new OperationalMetricEvent(
            DateTimeOffset.UtcNow,
            NormalizeCompanyId(companyId),
            null,
            MetricKinds.Sql,
            success,
            Math.Max(0, durationMs),
            null));

    public void RecordIntentCacheHit(int companyId) =>
        Add(new OperationalMetricEvent(
            DateTimeOffset.UtcNow,
            NormalizeCompanyId(companyId),
            null,
            MetricKinds.IntentCacheHit,
            true,
            0,
            null));

    public void RecordResultCacheHit(int companyId) =>
        Add(new OperationalMetricEvent(
            DateTimeOffset.UtcNow,
            NormalizeCompanyId(companyId),
            null,
            MetricKinds.ResultCacheHit,
            true,
            0,
            null));

    public OperationalMetricsSnapshot GetSnapshot(int companyId)
    {
        var now = DateTimeOffset.UtcNow;
        var normalizedCompanyId = NormalizeCompanyId(companyId);
        var from = now.Subtract(SnapshotWindow);
        OperationalMetricEvent[] events;

        lock (_sync)
        {
            Trim(now);
            events = _events
                .Where(item => item.CompanyId == normalizedCompanyId && item.At >= from)
                .ToArray();
        }

        var queryEvents = events.Where(item => item.Kind == MetricKinds.Query).ToArray();
        var llmEvents = events.Where(item => item.Kind == MetricKinds.Llm).ToArray();
        var sqlEvents = events.Where(item => item.Kind == MetricKinds.Sql).ToArray();
        var activeUsers = queryEvents
            .Where(item => !string.IsNullOrWhiteSpace(item.UserKey))
            .Select(item => item.UserKey)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .Count();

        return new OperationalMetricsSnapshot(
            StartedAt,
            now,
            queryEvents.Length,
            queryEvents.Count(item => item.Success),
            queryEvents.Count(item => !item.Success),
            events.Count(item => item.Kind == MetricKinds.RateLimited),
            activeUsers,
            llmEvents.Length,
            llmEvents.Count(item => !item.Success),
            sqlEvents.Length,
            sqlEvents.Count(item => !item.Success),
            events.Count(item => item.Kind == MetricKinds.IntentCacheHit),
            events.Count(item => item.Kind == MetricKinds.ResultCacheHit),
            Math.Max(0, Volatile.Read(ref _activeRequests)),
            Math.Max(0, Volatile.Read(ref _queuedLlmRequests)),
            Math.Max(0, Volatile.Read(ref _activeLlmRequests)),
            Math.Max(0, Volatile.Read(ref _queuedSqlQueries)),
            Math.Max(0, Volatile.Read(ref _activeSqlQueries)),
            Math.Max(0, Volatile.Read(ref _queuedBackgroundJobs)),
            Math.Max(0, Volatile.Read(ref _activeBackgroundJobs)),
            AverageDuration(llmEvents),
            AverageDuration(sqlEvents),
            AverageDuration(queryEvents),
            BuildBuckets(events, from, now));
    }

    private void Add(OperationalMetricEvent item)
    {
        lock (_sync)
        {
            _events.Enqueue(item);
            Trim(item.At);
        }
    }

    private void Trim(DateTimeOffset now)
    {
        var minAt = now.Subtract(Retention);
        while (_events.Count > 0 && (_events.Peek().At < minAt || _events.Count > MaxEvents))
            _events.Dequeue();
    }

    private static IReadOnlyList<OperationalMetricBucket> BuildBuckets(
        IReadOnlyList<OperationalMetricEvent> events,
        DateTimeOffset from,
        DateTimeOffset now)
    {
        var buckets = new List<OperationalMetricBucket>();
        for (var bucketFrom = AlignToBucket(from); bucketFrom < now; bucketFrom = bucketFrom.Add(BucketSize))
        {
            var bucketTo = bucketFrom.Add(BucketSize);
            var bucketEvents = events
                .Where(item => item.At >= bucketFrom && item.At < bucketTo)
                .ToArray();
            buckets.Add(new OperationalMetricBucket(
                bucketFrom,
                bucketTo,
                bucketEvents.Count(item => item.Kind == MetricKinds.Query),
                bucketEvents.Count(item =>
                    (item.Kind == MetricKinds.Query || item.Kind == MetricKinds.Llm || item.Kind == MetricKinds.Sql) &&
                    !item.Success),
                bucketEvents.Count(item => item.Kind == MetricKinds.RateLimited)));
        }

        return buckets;
    }

    private static DateTimeOffset AlignToBucket(DateTimeOffset value)
    {
        var ticks = value.UtcTicks - value.UtcTicks % BucketSize.Ticks;
        return new DateTimeOffset(ticks, TimeSpan.Zero);
    }

    private static double AverageDuration(IReadOnlyList<OperationalMetricEvent> events)
    {
        var timed = events.Where(item => item.DurationMs > 0).ToArray();
        return timed.Length == 0 ? 0 : Math.Round(timed.Average(item => item.DurationMs), 1);
    }

    private static int NormalizeCompanyId(int companyId) =>
        companyId <= 0 ? CompanyDefaults.DefaultCompanyId : companyId;

    private static string? NormalizeUser(string? userKey) =>
        string.IsNullOrWhiteSpace(userKey) ? null : userKey.Trim().ToLowerInvariant();

    private sealed record OperationalMetricEvent(
        DateTimeOffset At,
        int CompanyId,
        string? UserKey,
        string Kind,
        bool Success,
        long DurationMs,
        string? Provider);

    private static class MetricKinds
    {
        public const string Query = "query";
        public const string RateLimited = "rate_limited";
        public const string Llm = "llm";
        public const string Sql = "sql";
        public const string IntentCacheHit = "intent_cache_hit";
        public const string ResultCacheHit = "result_cache_hit";
    }

    private sealed class CounterScope : IDisposable
    {
        private readonly Action _onDispose;
        private int _disposed;

        public CounterScope(Action onDispose) => _onDispose = onDispose;

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) == 0)
                _onDispose();
        }
    }

    public sealed class QueueCounterScope : IDisposable
    {
        private readonly Action _leaveQueue;
        private readonly Action _enterActive;
        private readonly Action _leaveActive;
        private int _state;

        public QueueCounterScope(Action leaveQueue, Action enterActive, Action leaveActive)
        {
            _leaveQueue = leaveQueue;
            _enterActive = enterActive;
            _leaveActive = leaveActive;
        }

        public void MarkActive()
        {
            if (Interlocked.CompareExchange(ref _state, 1, 0) == 0)
            {
                _leaveQueue();
                _enterActive();
            }
        }

        public void Dispose()
        {
            var state = Interlocked.Exchange(ref _state, 2);
            if (state == 0)
                _leaveQueue();
            else if (state == 1)
                _leaveActive();
        }
    }
}
