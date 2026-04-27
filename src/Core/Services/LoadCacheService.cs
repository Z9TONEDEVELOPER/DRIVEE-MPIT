using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using NexusDataSpace.Core.Models;
using Microsoft.Extensions.Configuration;

namespace NexusDataSpace.Core.Services;

public sealed class LoadCacheService
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);
    private readonly ConcurrentDictionary<string, CacheEntry<QueryIntent>> _intentCache = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, CacheEntry<QueryResult>> _queryResultCache = new(StringComparer.Ordinal);
    private readonly TimeSpan _intentTtl;
    private readonly TimeSpan _queryResultTtl;
    private readonly int _maxEntries;

    public LoadCacheService(IConfiguration configuration)
    {
        _intentTtl = TimeSpan.FromSeconds(ReadBoundedInt(configuration, "Load:IntentCacheSeconds", 300, 0, 86_400));
        _queryResultTtl = TimeSpan.FromSeconds(ReadBoundedInt(configuration, "Load:ResultCacheSeconds", 60, 0, 3_600));
        _maxEntries = ReadBoundedInt(configuration, "Load:MaxCacheEntries", 300, 0, 10_000);
    }

    public bool TryGetIntent(string key, out QueryIntent? intent)
    {
        intent = null;
        if (_intentTtl <= TimeSpan.Zero || !_intentCache.TryGetValue(key, out var entry))
            return false;

        if (entry.ExpiresAt <= DateTimeOffset.UtcNow)
        {
            _intentCache.TryRemove(key, out _);
            return false;
        }

        intent = CloneIntent(entry.Value);
        return true;
    }

    public void SetIntent(string key, QueryIntent intent)
    {
        if (_intentTtl <= TimeSpan.Zero)
            return;

        TrimIfNeeded(_intentCache);
        _intentCache[key] = new CacheEntry<QueryIntent>(CloneIntent(intent), DateTimeOffset.UtcNow.Add(_intentTtl));
    }

    public bool TryGetQueryResult(string key, out QueryResult? result)
    {
        result = null;
        if (_queryResultTtl <= TimeSpan.Zero || !_queryResultCache.TryGetValue(key, out var entry))
            return false;

        if (entry.ExpiresAt <= DateTimeOffset.UtcNow)
        {
            _queryResultCache.TryRemove(key, out _);
            return false;
        }

        result = CloneQueryResult(entry.Value);
        return true;
    }

    public void SetQueryResult(string key, QueryResult result)
    {
        if (_queryResultTtl <= TimeSpan.Zero)
            return;

        TrimIfNeeded(_queryResultCache);
        _queryResultCache[key] = new CacheEntry<QueryResult>(CloneQueryResult(result), DateTimeOffset.UtcNow.Add(_queryResultTtl));
    }

    public static string BuildIntentKey(
        int companyId,
        string provider,
        string model,
        string userQuery,
        IReadOnlyList<ChatTurn>? history)
    {
        var source = new
        {
            companyId,
            provider,
            model,
            userQuery = NormalizeText(userQuery),
            history = history?
                .TakeLast(4)
                .Select(turn => new { role = turn.Role, content = NormalizeText(turn.Content) })
                .ToArray()
        };

        return "intent:" + Hash(JsonSerializer.Serialize(source, JsonOptions));
    }

    public static string BuildQueryResultKey(
        int companyId,
        int dataSourceId,
        string sqlSignature,
        IReadOnlyDictionary<string, object?> parameters)
    {
        var source = new
        {
            companyId,
            dataSourceId,
            sqlSignature,
            parameters = parameters
                .OrderBy(item => item.Key, StringComparer.Ordinal)
                .Select(item => new { name = item.Key, value = item.Value?.ToString() })
                .ToArray()
        };

        return "sql:" + Hash(JsonSerializer.Serialize(source, JsonOptions));
    }

    private static QueryIntent CloneIntent(QueryIntent intent) =>
        JsonSerializer.Deserialize<QueryIntent>(JsonSerializer.Serialize(intent, JsonOptions), JsonOptions)
        ?? new QueryIntent();

    private static QueryResult CloneQueryResult(QueryResult result) =>
        new()
        {
            Columns = result.Columns.ToList(),
            Rows = result.Rows.Select(row => row.ToList()).ToList(),
            DurationMs = result.DurationMs
        };

    private void TrimIfNeeded<T>(ConcurrentDictionary<string, CacheEntry<T>> cache)
    {
        if (_maxEntries <= 0 || cache.Count < _maxEntries)
            return;

        foreach (var key in cache
                     .OrderBy(item => item.Value.ExpiresAt)
                     .Take(Math.Max(1, cache.Count - _maxEntries + 1))
                     .Select(item => item.Key)
                     .ToArray())
        {
            cache.TryRemove(key, out _);
        }
    }

    private static string NormalizeText(string? value) =>
        string.IsNullOrWhiteSpace(value) ? "" : value.Trim().ToLowerInvariant();

    private static string Hash(string value) =>
        Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(value)));

    private static int ReadBoundedInt(IConfiguration configuration, string key, int fallback, int min, int max) =>
        int.TryParse(configuration[key], out var value) ? Math.Clamp(value, min, max) : fallback;

    private sealed record CacheEntry<T>(T Value, DateTimeOffset ExpiresAt);
}
