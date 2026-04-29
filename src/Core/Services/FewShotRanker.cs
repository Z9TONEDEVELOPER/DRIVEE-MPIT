using System.Globalization;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace NexusDataSpace.Core.Services;

public interface IFewShotRanker
{
    Task<IReadOnlyList<FewShotExample>> RankAsync(
        string userQuery,
        IReadOnlyList<FewShotExample> candidates,
        int topK,
        CancellationToken cancellationToken);
}

public static class FewShotRankerKinds
{
    public const string Off = "off";
    public const string Lexical = "lexical";
    public const string Embedding = "embedding";
}

public sealed class NullFewShotRanker : IFewShotRanker
{
    public Task<IReadOnlyList<FewShotExample>> RankAsync(
        string userQuery,
        IReadOnlyList<FewShotExample> candidates,
        int topK,
        CancellationToken cancellationToken)
    {
        if (topK <= 0 || candidates.Count == 0)
            return Task.FromResult<IReadOnlyList<FewShotExample>>(Array.Empty<FewShotExample>());

        return Task.FromResult<IReadOnlyList<FewShotExample>>(
            candidates.Count <= topK ? candidates : candidates.Take(topK).ToList());
    }
}

public sealed class LexicalFewShotRanker : IFewShotRanker
{
    private static readonly Regex TokenPattern = new(@"[\p{L}\p{N}_]+", RegexOptions.Compiled | RegexOptions.CultureInvariant);
    private static readonly HashSet<string> StopWords = new(StringComparer.OrdinalIgnoreCase)
    {
        "и", "а", "но", "или", "по", "за", "в", "на", "с", "от", "до", "для", "из", "под", "над",
        "сколько", "какие", "какой", "какая", "какое", "что", "как", "когда", "где", "у", "о",
        "the", "a", "an", "of", "for", "to", "in", "on", "by", "with", "from", "at", "and", "or"
    };

    private static readonly string[] StemSuffixes =
    {
        "ость", "ение", "ами", "ями", "ого", "его", "ой", "ей", "ие", "ые", "ая", "ую", "ом", "ах",
        "ям", "ев", "ов", "ы", "и", "а", "е", "у", "ю", "я", "ь"
    };

    public Task<IReadOnlyList<FewShotExample>> RankAsync(
        string userQuery,
        IReadOnlyList<FewShotExample> candidates,
        int topK,
        CancellationToken cancellationToken)
    {
        if (topK <= 0 || candidates.Count == 0)
            return Task.FromResult<IReadOnlyList<FewShotExample>>(Array.Empty<FewShotExample>());
        if (candidates.Count <= topK)
            return Task.FromResult<IReadOnlyList<FewShotExample>>(candidates);

        var queryStems = StemSet(Tokenize(userQuery));
        if (queryStems.Count == 0)
            return Task.FromResult<IReadOnlyList<FewShotExample>>(candidates.Take(topK).ToList());

        var ranked = candidates
            .Select((candidate, index) => new
            {
                Candidate = candidate,
                Score = ComputeJaccard(queryStems, StemSet(Tokenize(candidate.Query))),
                Index = index
            })
            .OrderByDescending(item => item.Score)
            .ThenBy(item => item.Index)
            .Take(topK)
            .Select(item => item.Candidate)
            .ToList();

        return Task.FromResult<IReadOnlyList<FewShotExample>>(ranked);
    }

    private static double ComputeJaccard(IReadOnlyCollection<string> a, IReadOnlyCollection<string> b)
    {
        if (a.Count == 0 || b.Count == 0)
            return 0;

        var intersection = a.Intersect(b, StringComparer.Ordinal).Count();
        if (intersection == 0)
            return 0;

        var union = a.Count + b.Count - intersection;
        return union == 0 ? 0 : (double)intersection / union;
    }

    private static List<string> Tokenize(string text)
    {
        var tokens = new List<string>();
        if (string.IsNullOrWhiteSpace(text))
            return tokens;

        foreach (Match match in TokenPattern.Matches(text.ToLowerInvariant()))
        {
            var token = match.Value;
            if (token.Length < 2 || StopWords.Contains(token))
                continue;
            tokens.Add(token);
        }

        return tokens;
    }

    private static HashSet<string> StemSet(IEnumerable<string> tokens) =>
        new(tokens.Select(Stem), StringComparer.Ordinal);

    private static string Stem(string token)
    {
        if (token.Length <= 4)
            return token;

        foreach (var suffix in StemSuffixes)
        {
            if (token.Length - suffix.Length >= 3 && token.EndsWith(suffix, StringComparison.Ordinal))
                return token[..^suffix.Length];
        }

        return token;
    }
}

public sealed class HttpEmbeddingFewShotRanker : IFewShotRanker
{
    private readonly HttpClient _httpClient;
    private readonly ILogger<HttpEmbeddingFewShotRanker> _logger;
    private readonly LexicalFewShotRanker _lexicalFallback;
    private readonly string _endpoint;
    private readonly string _model;
    private readonly string? _apiKey;
    private readonly TimeSpan _timeout;
    private readonly Dictionary<string, float[]> _cache = new(StringComparer.Ordinal);
    private readonly SemaphoreSlim _cacheGate = new(1, 1);

    public HttpEmbeddingFewShotRanker(
        IHttpClientFactory httpClientFactory,
        IConfiguration configuration,
        LexicalFewShotRanker lexicalFallback,
        ILogger<HttpEmbeddingFewShotRanker> logger)
    {
        _httpClient = httpClientFactory.CreateClient("llm");
        _lexicalFallback = lexicalFallback;
        _logger = logger;
        _endpoint = configuration["Llm:Embeddings:Endpoint"] ?? string.Empty;
        _model = configuration["Llm:Embeddings:Model"] ?? "text-embedding-3-small";
        _apiKey = configuration["Llm:Embeddings:ApiKey"];
        var timeoutSeconds = int.TryParse(configuration["Llm:Embeddings:TimeoutSeconds"], NumberStyles.Integer, CultureInfo.InvariantCulture, out var seconds)
            ? Math.Clamp(seconds, 1, 300)
            : 15;
        _timeout = TimeSpan.FromSeconds(timeoutSeconds);
    }

    public async Task<IReadOnlyList<FewShotExample>> RankAsync(
        string userQuery,
        IReadOnlyList<FewShotExample> candidates,
        int topK,
        CancellationToken cancellationToken)
    {
        if (topK <= 0 || candidates.Count == 0)
            return Array.Empty<FewShotExample>();
        if (candidates.Count <= topK)
            return candidates;
        if (string.IsNullOrWhiteSpace(_endpoint) || string.IsNullOrWhiteSpace(userQuery))
            return await _lexicalFallback.RankAsync(userQuery, candidates, topK, cancellationToken);

        try
        {
            using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            timeoutCts.CancelAfter(_timeout);

            var queryVector = await EmbedAsync(userQuery, timeoutCts.Token);
            if (queryVector == null)
                return await _lexicalFallback.RankAsync(userQuery, candidates, topK, cancellationToken);

            var scored = new List<(FewShotExample Example, double Score, int Index)>(candidates.Count);
            for (var index = 0; index < candidates.Count; index++)
            {
                var candidate = candidates[index];
                var candidateVector = await EmbedAsync(candidate.Query, timeoutCts.Token);
                var score = candidateVector == null ? 0 : CosineSimilarity(queryVector, candidateVector);
                scored.Add((candidate, score, index));
            }

            return scored
                .OrderByDescending(item => item.Score)
                .ThenBy(item => item.Index)
                .Take(topK)
                .Select(item => item.Example)
                .ToList();
        }
        catch (Exception exception) when (exception is HttpRequestException or TaskCanceledException or JsonException or InvalidOperationException)
        {
            _logger.LogWarning(exception, "Embedding ranker failed; falling back to lexical ranker.");
            return await _lexicalFallback.RankAsync(userQuery, candidates, topK, cancellationToken);
        }
    }

    private async Task<float[]?> EmbedAsync(string text, CancellationToken cancellationToken)
    {
        var trimmed = (text ?? string.Empty).Trim();
        if (trimmed.Length == 0)
            return null;

        await _cacheGate.WaitAsync(cancellationToken);
        try
        {
            if (_cache.TryGetValue(trimmed, out var cached))
                return cached;
        }
        finally
        {
            _cacheGate.Release();
        }

        var payload = JsonSerializer.Serialize(new { model = _model, input = trimmed });
        using var request = new HttpRequestMessage(HttpMethod.Post, _endpoint)
        {
            Content = new StringContent(payload, Encoding.UTF8, "application/json")
        };
        if (!string.IsNullOrWhiteSpace(_apiKey))
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", _apiKey);
        request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));

        using var response = await _httpClient.SendAsync(request, cancellationToken);
        var body = await response.Content.ReadAsStringAsync(cancellationToken);
        if (!response.IsSuccessStatusCode)
            throw new InvalidOperationException($"Embeddings HTTP {(int)response.StatusCode}: {body}");

        using var document = JsonDocument.Parse(body);
        if (!document.RootElement.TryGetProperty("data", out var data) ||
            data.ValueKind != JsonValueKind.Array ||
            data.GetArrayLength() == 0)
        {
            return null;
        }

        var first = data[0];
        if (!first.TryGetProperty("embedding", out var embeddingElement) ||
            embeddingElement.ValueKind != JsonValueKind.Array)
        {
            return null;
        }

        var vector = new float[embeddingElement.GetArrayLength()];
        var i = 0;
        foreach (var component in embeddingElement.EnumerateArray())
        {
            vector[i++] = component.ValueKind switch
            {
                JsonValueKind.Number => (float)component.GetDouble(),
                _ => 0
            };
        }

        await _cacheGate.WaitAsync(cancellationToken);
        try
        {
            _cache[trimmed] = vector;
        }
        finally
        {
            _cacheGate.Release();
        }

        return vector;
    }

    private static double CosineSimilarity(float[] a, float[] b)
    {
        if (a.Length == 0 || b.Length == 0 || a.Length != b.Length)
            return 0;

        double dot = 0, normA = 0, normB = 0;
        for (var i = 0; i < a.Length; i++)
        {
            dot += a[i] * b[i];
            normA += a[i] * a[i];
            normB += b[i] * b[i];
        }

        if (normA <= 0 || normB <= 0)
            return 0;

        return dot / (Math.Sqrt(normA) * Math.Sqrt(normB));
    }
}

public static class FewShotRankerFactory
{
    public static IFewShotRanker Create(IServiceProvider services, IConfiguration configuration)
    {
        var kind = (configuration["Llm:FewShotRanker"] ?? FewShotRankerKinds.Lexical).Trim().ToLowerInvariant();
        return kind switch
        {
            FewShotRankerKinds.Off => (IFewShotRanker)services.GetService(typeof(NullFewShotRanker))! ?? new NullFewShotRanker(),
            FewShotRankerKinds.Embedding => (IFewShotRanker)services.GetService(typeof(HttpEmbeddingFewShotRanker))!,
            _ => (IFewShotRanker)services.GetService(typeof(LexicalFewShotRanker))!
        };
    }
}
