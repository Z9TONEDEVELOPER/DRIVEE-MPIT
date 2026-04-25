using System.Collections.Concurrent;
using DriveeDataSpace.Core.Models;
using Microsoft.Extensions.Configuration;

namespace DriveeDataSpace.Core.Services;

public sealed class QueryLoadControl
{
    private readonly ConcurrentDictionary<string, SlidingWindowCounter> _counters = new(StringComparer.Ordinal);
    private readonly int _userQueriesPerWindow;
    private readonly int _companyQueriesPerWindow;
    private readonly TimeSpan _window;

    public QueryLoadControl(IConfiguration configuration)
    {
        _userQueriesPerWindow = ReadBoundedInt(configuration, "Load:UserQueriesPerMinute", 12, 1, 10_000);
        _companyQueriesPerWindow = ReadBoundedInt(configuration, "Load:CompanyQueriesPerMinute", 60, 1, 100_000);
        _window = TimeSpan.FromSeconds(ReadBoundedInt(configuration, "Load:RateLimitWindowSeconds", 60, 10, 3_600));
    }

    public bool TryAcquire(int companyId, string? userKey, out TimeSpan retryAfter)
    {
        var now = DateTimeOffset.UtcNow;
        var companyPart = companyId <= 0 ? CompanyDefaults.DefaultCompanyId : companyId;
        var normalizedUser = NormalizeUserKey(userKey);

        if (!TryAcquireKey($"company:{companyPart}:user:{normalizedUser}", _userQueriesPerWindow, now, out retryAfter))
            return false;

        if (!TryAcquireKey($"company:{companyPart}", _companyQueriesPerWindow, now, out retryAfter))
            return false;

        retryAfter = TimeSpan.Zero;
        return true;
    }

    private bool TryAcquireKey(string key, int limit, DateTimeOffset now, out TimeSpan retryAfter)
    {
        var counter = _counters.GetOrAdd(key, static _ => new SlidingWindowCounter());
        return counter.TryAcquire(now, _window, limit, out retryAfter);
    }

    private static string NormalizeUserKey(string? userKey) =>
        string.IsNullOrWhiteSpace(userKey)
            ? "unknown"
            : userKey.Trim().ToLowerInvariant();

    private static int ReadBoundedInt(IConfiguration configuration, string key, int fallback, int min, int max) =>
        int.TryParse(configuration[key], out var value) ? Math.Clamp(value, min, max) : fallback;

    private sealed class SlidingWindowCounter
    {
        private readonly object _sync = new();
        private readonly Queue<DateTimeOffset> _hits = new();

        public bool TryAcquire(DateTimeOffset now, TimeSpan window, int limit, out TimeSpan retryAfter)
        {
            lock (_sync)
            {
                while (_hits.Count > 0 && now - _hits.Peek() >= window)
                    _hits.Dequeue();

                if (_hits.Count >= limit)
                {
                    retryAfter = window - (now - _hits.Peek());
                    if (retryAfter < TimeSpan.Zero)
                        retryAfter = TimeSpan.Zero;
                    return false;
                }

                _hits.Enqueue(now);
                retryAfter = TimeSpan.Zero;
                return true;
            }
        }
    }
}
