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

    public DateTimeOffset StartedAt { get; } = DateTimeOffset.UtcNow;

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
}
