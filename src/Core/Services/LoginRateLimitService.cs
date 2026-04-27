using System.Collections.Concurrent;
using NexusDataSpace.Core.Models;
using Microsoft.Extensions.Configuration;

namespace NexusDataSpace.Core.Services;

public sealed class LoginRateLimitService
{
    private readonly ConcurrentDictionary<string, SlidingWindowCounter> _counters = new(StringComparer.Ordinal);
    private readonly int _usernameAttemptsPerWindow;
    private readonly int _ipAttemptsPerWindow;
    private readonly TimeSpan _window;

    public LoginRateLimitService(IConfiguration configuration)
    {
        _usernameAttemptsPerWindow = ReadBoundedInt(configuration, "Security:LoginAttemptsPerMinute", 5, 1, 1_000);
        _ipAttemptsPerWindow = ReadBoundedInt(configuration, "Security:LoginIpAttemptsPerMinute", 30, 1, 10_000);
        _window = TimeSpan.FromSeconds(ReadBoundedInt(configuration, "Security:LoginRateLimitWindowSeconds", 60, 10, 3_600));
    }

    public bool TryAcquire(string? username, string? ipAddress, out TimeSpan retryAfter)
    {
        var now = DateTimeOffset.UtcNow;
        var normalizedUsername = string.IsNullOrWhiteSpace(username)
            ? "unknown"
            : username.Trim().ToLowerInvariant();
        var normalizedIp = string.IsNullOrWhiteSpace(ipAddress)
            ? "unknown"
            : ipAddress.Trim().ToLowerInvariant();

        if (!TryAcquireKey($"login:user:{normalizedUsername}", _usernameAttemptsPerWindow, now, out retryAfter))
            return false;

        if (!TryAcquireKey($"login:ip:{normalizedIp}", _ipAttemptsPerWindow, now, out retryAfter))
            return false;

        retryAfter = TimeSpan.Zero;
        return true;
    }

    private bool TryAcquireKey(string key, int limit, DateTimeOffset now, out TimeSpan retryAfter)
    {
        var counter = _counters.GetOrAdd(key, static _ => new SlidingWindowCounter());
        return counter.TryAcquire(now, _window, limit, out retryAfter);
    }

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
