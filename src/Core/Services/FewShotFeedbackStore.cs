using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using NexusDataSpace.Core.Models;

namespace NexusDataSpace.Core.Services;

public interface IFewShotFeedbackStore
{
    void Save(int companyId, string query, QueryIntent correctedIntent);
    IReadOnlyList<FewShotExample> GetForCompany(int companyId, int max);
}

public sealed class JsonFileFewShotFeedbackStore : IFewShotFeedbackStore
{
    private static readonly JsonSerializerOptions IntentSerializerOptions = new()
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        WriteIndented = false,
        Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping
    };

    private static readonly JsonSerializerOptions EntrySerializerOptions = new()
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        WriteIndented = false,
        Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping
    };

    private readonly ILogger<JsonFileFewShotFeedbackStore> _logger;
    private readonly bool _enabled;
    private readonly string _storagePath;
    private readonly int _maxPerCompany;
    private readonly object _gate = new();
    private Dictionary<int, List<StoredFeedback>>? _cache;

    public JsonFileFewShotFeedbackStore(IConfiguration configuration, ILogger<JsonFileFewShotFeedbackStore> logger)
    {
        _logger = logger;
        _enabled = !bool.TryParse(configuration["Llm:FewShotFeedback:Enabled"], out var enabled) || enabled;
        _storagePath = ResolveStoragePath(configuration["Llm:FewShotFeedback:StoragePath"]);
        _maxPerCompany = int.TryParse(configuration["Llm:FewShotFeedback:MaxPerCompany"], NumberStyles.Integer, CultureInfo.InvariantCulture, out var max)
            ? Math.Clamp(max, 1, 1000)
            : 200;
    }

    public void Save(int companyId, string query, QueryIntent correctedIntent)
    {
        if (!_enabled || string.IsNullOrWhiteSpace(query))
            return;

        var trimmedQuery = query.Trim();
        var intentJson = JsonSerializer.Serialize(correctedIntent, IntentSerializerOptions);
        var entry = new StoredFeedback(companyId, trimmedQuery, intentJson, DateTime.UtcNow);

        try
        {
            lock (_gate)
            {
                EnsureLoadedLocked();
                if (!_cache!.TryGetValue(companyId, out var bucket))
                {
                    bucket = new List<StoredFeedback>();
                    _cache[companyId] = bucket;
                }

                bucket.RemoveAll(item => string.Equals(item.Query, trimmedQuery, StringComparison.OrdinalIgnoreCase));
                bucket.Add(entry);
                while (bucket.Count > _maxPerCompany)
                    bucket.RemoveAt(0);

                PersistLocked();
            }
        }
        catch (Exception exception)
        {
            _logger.LogWarning(exception, "Failed to persist few-shot feedback to {Path}.", _storagePath);
        }
    }

    public IReadOnlyList<FewShotExample> GetForCompany(int companyId, int max)
    {
        if (!_enabled || max <= 0)
            return Array.Empty<FewShotExample>();

        lock (_gate)
        {
            EnsureLoadedLocked();
            if (!_cache!.TryGetValue(companyId, out var bucket) || bucket.Count == 0)
                return Array.Empty<FewShotExample>();

            return bucket
                .AsEnumerable()
                .Reverse()
                .Take(max)
                .Select(entry => new FewShotExample(entry.Query, entry.IntentJson))
                .ToList();
        }
    }

    private void EnsureLoadedLocked()
    {
        if (_cache != null)
            return;

        _cache = new Dictionary<int, List<StoredFeedback>>();
        if (!File.Exists(_storagePath))
            return;

        try
        {
            foreach (var line in File.ReadAllLines(_storagePath))
            {
                if (string.IsNullOrWhiteSpace(line))
                    continue;

                var entry = JsonSerializer.Deserialize<StoredFeedback>(line, EntrySerializerOptions);
                if (entry == null || string.IsNullOrWhiteSpace(entry.Query))
                    continue;

                if (!_cache.TryGetValue(entry.CompanyId, out var bucket))
                {
                    bucket = new List<StoredFeedback>();
                    _cache[entry.CompanyId] = bucket;
                }

                bucket.Add(entry);
            }

            foreach (var (_, bucket) in _cache)
            {
                while (bucket.Count > _maxPerCompany)
                    bucket.RemoveAt(0);
            }
        }
        catch (Exception exception)
        {
            _logger.LogWarning(exception, "Failed to read few-shot feedback from {Path}; starting empty.", _storagePath);
            _cache = new Dictionary<int, List<StoredFeedback>>();
        }
    }

    private void PersistLocked()
    {
        var directory = Path.GetDirectoryName(_storagePath);
        if (!string.IsNullOrWhiteSpace(directory) && !Directory.Exists(directory))
            Directory.CreateDirectory(directory);

        using var writer = new StreamWriter(_storagePath, append: false);
        foreach (var (_, bucket) in _cache!)
        {
            foreach (var entry in bucket)
                writer.WriteLine(JsonSerializer.Serialize(entry, EntrySerializerOptions));
        }
    }

    private static string ResolveStoragePath(string? configured)
    {
        var path = string.IsNullOrWhiteSpace(configured) ? "Data/few-shot-feedback.jsonl" : configured.Trim();
        return Path.IsPathRooted(path)
            ? path
            : Path.Combine(AppContext.BaseDirectory, path);
    }

    private sealed record StoredFeedback(
        [property: JsonPropertyName("company")] int CompanyId,
        [property: JsonPropertyName("query")] string Query,
        [property: JsonPropertyName("intent")] string IntentJson,
        [property: JsonPropertyName("captured_at")] DateTime CapturedAt);
}

public sealed class CompositeFewShotProvider : IFewShotProvider
{
    private readonly StaticFewShotProvider _staticProvider;
    private readonly IFewShotFeedbackStore _feedbackStore;
    private readonly TenantContext _tenantContext;

    public CompositeFewShotProvider(
        StaticFewShotProvider staticProvider,
        IFewShotFeedbackStore feedbackStore,
        TenantContext tenantContext)
    {
        _staticProvider = staticProvider;
        _feedbackStore = feedbackStore;
        _tenantContext = tenantContext;
    }

    public IReadOnlyList<FewShotExample> SelectFor(string userQuery, int max)
    {
        if (max <= 0)
            return Array.Empty<FewShotExample>();

        var feedback = _feedbackStore.GetForCompany(_tenantContext.CompanyId, max);
        if (feedback.Count >= max)
            return feedback;

        var seenQueries = new HashSet<string>(
            feedback.Select(item => item.Query),
            StringComparer.OrdinalIgnoreCase);

        var combined = new List<FewShotExample>(feedback);
        var staticExamples = _staticProvider.SelectFor(userQuery, max);
        foreach (var example in staticExamples)
        {
            if (combined.Count >= max)
                break;
            if (seenQueries.Add(example.Query))
                combined.Add(example);
        }

        return combined;
    }
}
