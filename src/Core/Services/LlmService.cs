using System.Globalization;
using System.Net.Http.Headers;
using System.Diagnostics;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using NexusDataSpace.Core.Models;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace NexusDataSpace.Core.Services;

public class LlmService
{
    private readonly HttpClient _httpClient;
    private readonly ILogger<LlmService> _logger;
    private readonly LlmSettingsService _settings;
    private readonly TenantContext _tenantContext;
    private readonly LoadCacheService _cache;
    private readonly OperationalMetricsService _metrics;
    private readonly string _endpoint;
    private readonly string _model;
    private readonly bool _usesSimpleChatApi;
    private readonly SemaphoreSlim _localLlmSemaphore;
    private readonly SemaphoreSlim _gigaChatSemaphore;
    private readonly TimeSpan _llmQueueTimeout;
    private readonly int _maxHistoryTurns;
    private readonly int _maxOutputTokens;
    private readonly double _temperature;
    private readonly bool _fastHeuristicEnabled;
    private readonly double _fastHeuristicConfidenceThreshold;
    private readonly string _fallbackProvider;
    private readonly string _gigaChatScope;
    private readonly string _gigaChatAuthUrl;
    private readonly string _gigaChatBaseUrl;
    private readonly string _gigaChatModel;
    private string? _gigaChatAccessToken;
    private string? _gigaChatAccessTokenKeyFingerprint;
    private DateTimeOffset _gigaChatTokenExpiresAt;

    public LlmService(
        IHttpClientFactory httpClientFactory,
        IConfiguration configuration,
        LlmSettingsService settings,
        TenantContext tenantContext,
        LoadCacheService cache,
        OperationalMetricsService metrics,
        ILogger<LlmService> logger)
    {
        _httpClient = httpClientFactory.CreateClient("llm");
        _settings = settings;
        _tenantContext = tenantContext;
        _cache = cache;
        _metrics = metrics;
        _logger = logger;
        _endpoint = configuration["Llm:Endpoint"] ?? "http://localhost:1234/api/v1/chat";
        _model = configuration["Llm:Model"] ?? "qwen2.5-coder-7b-instruct";
        _usesSimpleChatApi = _endpoint.Contains("/api/v1/chat", StringComparison.OrdinalIgnoreCase);
        _localLlmSemaphore = new SemaphoreSlim(ReadBoundedInt(configuration, "Load:MaxConcurrentLocalLlmRequests", 1, 1, 32));
        _gigaChatSemaphore = new SemaphoreSlim(ReadBoundedInt(configuration, "Load:MaxConcurrentGigaChatRequests", 4, 1, 64));
        _llmQueueTimeout = TimeSpan.FromSeconds(ReadBoundedInt(configuration, "Load:LlmQueueTimeoutSeconds", 10, 1, 300));
        _maxHistoryTurns = ReadBoundedInt(configuration, "Llm:MaxHistoryTurns", 2, 0, 8);
        _maxOutputTokens = ReadBoundedInt(configuration, "Llm:MaxOutputTokens", 700, 128, 2_048);
        _temperature = ReadBoundedDouble(configuration, "Llm:Temperature", 0, 0, 1);
        _fastHeuristicEnabled = !bool.TryParse(configuration["Llm:FastHeuristicEnabled"], out var fastHeuristicEnabled) || fastHeuristicEnabled;
        _fastHeuristicConfidenceThreshold = ReadBoundedDouble(configuration, "Llm:FastHeuristicConfidenceThreshold", 0.7, 0.5, 0.95);
        _fallbackProvider = LlmProviders.Normalize(configuration["Llm:Fallback:Provider"]);
        _gigaChatScope = configuration["Llm:Fallback:GigaChat:Scope"] ?? "GIGACHAT_API_PERS";
        _gigaChatAuthUrl = configuration["Llm:Fallback:GigaChat:AuthUrl"] ?? "https://ngw.devices.sberbank.ru:9443/api/v2/oauth";
        _gigaChatBaseUrl = (configuration["Llm:Fallback:GigaChat:BaseUrl"] ?? "https://gigachat.devices.sberbank.ru/api/v1").TrimEnd('/');
        _gigaChatModel = configuration["Llm:Fallback:GigaChat:Model"] ?? "GigaChat";
        var timeoutSeconds = int.TryParse(configuration["Llm:TimeoutSeconds"], out var timeout) ? timeout : 60;
        _httpClient.Timeout = TimeSpan.FromSeconds(timeoutSeconds);
    }

    public async Task<QueryIntent> InterpretAsync(
        string userQuery,
        string systemPrompt,
        IReadOnlyList<ChatTurn>? history = null,
        CancellationToken cancellationToken = default)
    {
        if (TryResolveClarificationAnswer(userQuery, history) is { } clarifiedIntent)
        {
            _logger.LogInformation(
                "Clarification answer resolved without LLM. kind={Kind}, confidence={Confidence:P0}",
                clarifiedIntent.Kind,
                clarifiedIntent.Confidence);
            return clarifiedIntent;
        }

        if (_fastHeuristicEnabled && TryFastHeuristic(userQuery) is { } fastIntent)
        {
            _logger.LogInformation(
                "Fast heuristic resolved query without LLM. kind={Kind}, confidence={Confidence:P0}",
                fastIntent.Kind,
                fastIntent.Confidence);
            return fastIntent;
        }

        var provider = _settings.GetProvider();
        var cacheKey = LoadCacheService.BuildIntentKey(
            _tenantContext.CompanyId,
            provider,
            string.Equals(provider, LlmProviders.GigaChat, StringComparison.OrdinalIgnoreCase) ? _gigaChatModel : _model,
            userQuery,
            history);
        if (_cache.TryGetIntent(cacheKey, out var cachedIntent) && cachedIntent != null)
        {
            _metrics.RecordIntentCacheHit(_tenantContext.CompanyId);
            _logger.LogInformation("LLM intent cache hit. provider={Provider}, company_id={CompanyId}", provider, _tenantContext.CompanyId);
            return cachedIntent;
        }

        QueryIntent intent;
        if (string.Equals(provider, LlmProviders.GigaChat, StringComparison.OrdinalIgnoreCase))
        {
            intent = await InterpretWithGigaChatOrFallbackAsync(userQuery, systemPrompt, history, cancellationToken);
        }
        else
        {
            intent = await InterpretWithLocalOrFallbackAsync(userQuery, systemPrompt, history, cancellationToken);
        }

        _cache.SetIntent(cacheKey, intent);
        return intent;
    }

    private async Task<QueryIntent> InterpretWithLocalOrFallbackAsync(
        string userQuery,
        string systemPrompt,
        IReadOnlyList<ChatTurn>? history,
        CancellationToken cancellationToken)
    {
        try
        {
            return await InterpretWithLocalAsync(userQuery, systemPrompt, history, cancellationToken);
        }
        catch (Exception exception) when (exception is HttpRequestException or TaskCanceledException or JsonException or InvalidOperationException)
        {
            _logger.LogWarning(exception, "Local LLM interpretation failed.");
            var fallbackIntent = await TryFallbackInterpretAsync(userQuery, systemPrompt, history, cancellationToken);
            if (fallbackIntent != null)
                return fallbackIntent;

            _logger.LogWarning("LLM fallback is unavailable. Falling back to heuristic parser.");
            return HeuristicFallback(userQuery);
        }
    }

    private async Task<QueryIntent> InterpretWithGigaChatOrFallbackAsync(
        string userQuery,
        string systemPrompt,
        IReadOnlyList<ChatTurn>? history,
        CancellationToken cancellationToken)
    {
        var authorizationKey = _settings.GetGigaChatAuthorizationKey();
        if (string.IsNullOrWhiteSpace(authorizationKey))
            throw new InvalidOperationException("GigaChat provider is selected, but Llm:Fallback:GigaChat:AuthorizationKey is empty.");

        try
        {
            return await InterpretWithGigaChatAsync(userQuery, systemPrompt, history, authorizationKey, cancellationToken);
        }
        catch (Exception exception) when (exception is HttpRequestException or TaskCanceledException or JsonException or InvalidOperationException)
        {
            _logger.LogWarning(exception, "GigaChat interpretation failed. Falling back to heuristic parser.");
            return HeuristicFallback(userQuery);
        }
    }

    private async Task<QueryIntent> InterpretWithLocalAsync(
        string userQuery,
        string systemPrompt,
        IReadOnlyList<ChatTurn>? history,
        CancellationToken cancellationToken)
    {
        using var llmScope = _metrics.EnterLlmQueue();
        if (!await _localLlmSemaphore.WaitAsync(_llmQueueTimeout, cancellationToken))
            throw new InvalidOperationException("Local LLM is busy. Try again in a few seconds.");
        llmScope.MarkActive();

        try
        {
            var payload = BuildLocalPayload(userQuery, systemPrompt, history);
            using var request = new HttpRequestMessage(HttpMethod.Post, _endpoint)
            {
                Content = new StringContent(JsonSerializer.Serialize(payload), Encoding.UTF8, "application/json")
            };

            var stopwatch = Stopwatch.StartNew();
            var recorded = false;
            using var response = await _httpClient.SendAsync(request, cancellationToken);
            var responseBody = await response.Content.ReadAsStringAsync(cancellationToken);
            stopwatch.Stop();
            _metrics.RecordLlmCall(_tenantContext.CompanyId, LlmProviders.Local, response.IsSuccessStatusCode, stopwatch.ElapsedMilliseconds);
            recorded = true;
            _logger.LogInformation(
                "Local LLM HTTP call completed in {ElapsedMs} ms. status={StatusCode}, model={Model}",
                stopwatch.ElapsedMilliseconds,
                (int)response.StatusCode,
                _model);
            if (!response.IsSuccessStatusCode)
                throw new InvalidOperationException($"LLM HTTP {(int)response.StatusCode}: {responseBody}");

            try
            {
                return ParseIntentResponse(responseBody) ?? HeuristicFallback(userQuery);
            }
            catch
            {
                if (!recorded)
                    _metrics.RecordLlmCall(_tenantContext.CompanyId, LlmProviders.Local, false, stopwatch.ElapsedMilliseconds);
                throw;
            }
        }
        finally
        {
            _localLlmSemaphore.Release();
        }
    }

    private async Task<QueryIntent?> TryFallbackInterpretAsync(
        string userQuery,
        string systemPrompt,
        IReadOnlyList<ChatTurn>? history,
        CancellationToken cancellationToken)
    {
        var authorizationKey = _settings.GetGigaChatAuthorizationKey();
        if (!string.Equals(_fallbackProvider, LlmProviders.GigaChat, StringComparison.OrdinalIgnoreCase) ||
            string.IsNullOrWhiteSpace(authorizationKey))
        {
            return null;
        }

        try
        {
            return await InterpretWithGigaChatAsync(userQuery, systemPrompt, history, authorizationKey, cancellationToken);
        }
        catch (Exception exception) when (exception is HttpRequestException or TaskCanceledException or JsonException or InvalidOperationException)
        {
            _logger.LogWarning(exception, "GigaChat fallback interpretation failed.");
            return null;
        }
    }

    private async Task<QueryIntent> InterpretWithGigaChatAsync(
        string userQuery,
        string systemPrompt,
        IReadOnlyList<ChatTurn>? history,
        string authorizationKey,
        CancellationToken cancellationToken)
    {
        using var llmScope = _metrics.EnterLlmQueue();
        if (!await _gigaChatSemaphore.WaitAsync(_llmQueueTimeout, cancellationToken))
            throw new InvalidOperationException("GigaChat queue is busy. Try again in a few seconds.");
        llmScope.MarkActive();

        try
        {
            var accessToken = await GetGigaChatAccessTokenAsync(authorizationKey, cancellationToken);
            var messages = new List<object> { new { role = "system", content = systemPrompt } };
            if (history != null)
            {
                foreach (var turn in history.TakeLast(_maxHistoryTurns))
                    messages.Add(new { role = turn.Role, content = turn.Content });
            }

            messages.Add(new { role = "user", content = userQuery });
            var payload = new
            {
                model = _gigaChatModel,
                messages,
                temperature = _temperature,
                max_tokens = _maxOutputTokens
            };

            using var request = new HttpRequestMessage(HttpMethod.Post, $"{_gigaChatBaseUrl}/chat/completions")
            {
                Content = new StringContent(JsonSerializer.Serialize(payload), Encoding.UTF8, "application/json")
            };
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", accessToken);
            request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));

            var stopwatch = Stopwatch.StartNew();
            using var response = await _httpClient.SendAsync(request, cancellationToken);
            var responseBody = await response.Content.ReadAsStringAsync(cancellationToken);
            stopwatch.Stop();
            _metrics.RecordLlmCall(_tenantContext.CompanyId, LlmProviders.GigaChat, response.IsSuccessStatusCode, stopwatch.ElapsedMilliseconds);
            _logger.LogInformation(
                "GigaChat LLM HTTP call completed in {ElapsedMs} ms. status={StatusCode}, model={Model}",
                stopwatch.ElapsedMilliseconds,
                (int)response.StatusCode,
                _gigaChatModel);
            if (!response.IsSuccessStatusCode)
                throw new InvalidOperationException($"GigaChat HTTP {(int)response.StatusCode}: {responseBody}");

            return ParseIntentResponse(responseBody) ?? HeuristicFallback(userQuery);
        }
        finally
        {
            _gigaChatSemaphore.Release();
        }
    }

    private async Task<string> GetGigaChatAccessTokenAsync(string authorizationKey, CancellationToken cancellationToken)
    {
        var keyFingerprint = FingerprintSecret(authorizationKey);
        if (!string.IsNullOrWhiteSpace(_gigaChatAccessToken) &&
            string.Equals(_gigaChatAccessTokenKeyFingerprint, keyFingerprint, StringComparison.Ordinal) &&
            _gigaChatTokenExpiresAt > DateTimeOffset.UtcNow.AddMinutes(1))
        {
            return _gigaChatAccessToken;
        }

        using var request = new HttpRequestMessage(HttpMethod.Post, _gigaChatAuthUrl)
        {
            Content = new FormUrlEncodedContent(new Dictionary<string, string>
            {
                ["scope"] = _gigaChatScope
            })
        };
        request.Headers.Authorization = new AuthenticationHeaderValue("Basic", authorizationKey);
        request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
        request.Headers.Add("RqUID", Guid.NewGuid().ToString());

        using var response = await _httpClient.SendAsync(request, cancellationToken);
        var body = await response.Content.ReadAsStringAsync(cancellationToken);
        if (!response.IsSuccessStatusCode)
            throw new InvalidOperationException($"GigaChat OAuth HTTP {(int)response.StatusCode}: {body}");

        using var document = JsonDocument.Parse(body);
        _gigaChatAccessToken = document.RootElement.GetProperty("access_token").GetString()
            ?? throw new InvalidOperationException("GigaChat OAuth response does not contain access_token.");
        _gigaChatAccessTokenKeyFingerprint = keyFingerprint;

        var expiresAt = document.RootElement.TryGetProperty("expires_at", out var expiresAtElement) && expiresAtElement.TryGetInt64(out var unix)
            ? DateTimeOffset.FromUnixTimeSeconds(unix)
            : DateTimeOffset.UtcNow.AddMinutes(25);
        _gigaChatTokenExpiresAt = expiresAt;

        return _gigaChatAccessToken;
    }

    private static string FingerprintSecret(string value)
        => Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(value)));

    private static string ExtractContent(string responseBody)
    {
        using var document = JsonDocument.Parse(responseBody);
        var root = document.RootElement;

        if (root.TryGetProperty("choices", out var choices) &&
            choices.ValueKind == JsonValueKind.Array &&
            choices.GetArrayLength() > 0)
        {
            var firstChoice = choices[0];
            if (firstChoice.TryGetProperty("message", out var message) &&
                message.TryGetProperty("content", out var content))
            {
                return content.GetString() ?? string.Empty;
            }

            if (firstChoice.TryGetProperty("text", out var text))
                return text.GetString() ?? string.Empty;
        }

        if (root.TryGetProperty("output", out var output))
        {
            if (output.ValueKind == JsonValueKind.String)
                return output.GetString() ?? string.Empty;

            if (output.ValueKind == JsonValueKind.Array)
            {
                string? messageContent = null;
                var chunks = new List<string>();
                foreach (var item in output.EnumerateArray())
                {
                    if (!item.TryGetProperty("content", out var contentElement) || contentElement.ValueKind != JsonValueKind.String)
                        continue;

                    var content = contentElement.GetString();
                    if (string.IsNullOrWhiteSpace(content))
                        continue;

                    chunks.Add(content);

                    if (item.TryGetProperty("type", out var typeElement) &&
                        string.Equals(typeElement.GetString(), "message", StringComparison.OrdinalIgnoreCase))
                    {
                        messageContent = content;
                    }
                }

                return messageContent ?? string.Join("\n", chunks);
            }
        }

        if (root.TryGetProperty("response", out var response) && response.ValueKind == JsonValueKind.String)
            return response.GetString() ?? string.Empty;

        return responseBody;
    }

    private object BuildLocalPayload(string userQuery, string systemPrompt, IReadOnlyList<ChatTurn>? history)
    {
        if (_usesSimpleChatApi)
        {
            return new
            {
                model = _model,
                system_prompt = systemPrompt,
                input = BuildFlatInput(userQuery, history, _maxHistoryTurns)
            };
        }

        var messages = new List<object> { new { role = "system", content = systemPrompt } };
        if (history != null)
        {
            foreach (var turn in history.TakeLast(_maxHistoryTurns))
                messages.Add(new { role = turn.Role, content = turn.Content });
        }

        messages.Add(new { role = "user", content = userQuery });
        return new
        {
            model = _model,
            temperature = _temperature,
            max_tokens = _maxOutputTokens,
            messages = messages.ToArray()
        };
    }

    private static QueryIntent? ParseIntentResponse(string responseBody)
    {
        var content = ExtractContent(responseBody);
        var json = ExtractJson(content);
        return JsonSerializer.Deserialize<QueryIntent>(json, new JsonSerializerOptions
        {
            PropertyNameCaseInsensitive = true
        });
    }

    private QueryIntent? TryFastHeuristic(string userQuery)
    {
        var intent = HeuristicFallback(userQuery);
        if (string.Equals(intent.Kind, QueryIntentKinds.Chat, StringComparison.OrdinalIgnoreCase))
            return intent;

        return IsFastHeuristicIntent(intent) && intent.Confidence >= _fastHeuristicConfidenceThreshold
            ? intent
            : null;
    }

    private static QueryIntent? TryResolveClarificationAnswer(string userQuery, IReadOnlyList<ChatTurn>? history)
    {
        if (history == null || history.Count == 0)
            return null;

        for (var i = history.Count - 1; i >= 0; i--)
        {
            var turn = history[i];
            if (!IsAssistantTurn(turn) ||
                !PeriodTextPatterns.TryResolveClarificationPeriod(userQuery, turn.Content, out var resolvedPeriod))
            {
                continue;
            }

            var previousUserQuery = FindPreviousUserQuery(history, i);
            if (string.IsNullOrWhiteSpace(previousUserQuery))
                return null;

            var resolvedQuery = $"{previousUserQuery} {resolvedPeriod}";
            var intent = HeuristicFallback(resolvedQuery);
            if (!IsFastHeuristicIntent(intent))
                return null;

            intent.Confidence = Math.Max(intent.Confidence, 0.75);
            intent.Explanation = $"Resolved period clarification using previous query and `{resolvedPeriod}`.";
            return intent;
        }

        return null;
    }

    private static string? FindPreviousUserQuery(IReadOnlyList<ChatTurn> history, int beforeIndex)
    {
        for (var i = beforeIndex - 1; i >= 0; i--)
        {
            if (IsUserTurn(history[i]) && !string.IsNullOrWhiteSpace(history[i].Content))
                return history[i].Content.Trim();
        }

        return null;
    }

    private static bool IsAssistantTurn(ChatTurn turn) =>
        string.Equals(turn.Role, "assistant", StringComparison.OrdinalIgnoreCase);

    private static bool IsUserTurn(ChatTurn turn) =>
        string.Equals(turn.Role, "user", StringComparison.OrdinalIgnoreCase);

    private static bool IsFastHeuristicIntent(QueryIntent intent) =>
        string.Equals(intent.Kind, QueryIntentKinds.Query, StringComparison.OrdinalIgnoreCase) &&
        !string.IsNullOrWhiteSpace(intent.Metric) &&
        (intent.Dimensions.Count > 0 ||
         intent.DateRange != null ||
         intent.Filters.Count > 0 ||
         intent.Limit.HasValue ||
         intent.Periods?.Count > 0);

    private static string BuildFlatInput(string userQuery, IReadOnlyList<ChatTurn>? history, int maxHistoryTurns)
    {
        if (history == null || history.Count == 0 || maxHistoryTurns <= 0)
            return userQuery;

        var builder = new StringBuilder();
        builder.AppendLine("Conversation context:");

        foreach (var turn in history.TakeLast(maxHistoryTurns))
        {
            builder
                .Append("- ")
                .Append(turn.Role)
                .Append(": ")
                .AppendLine(turn.Content);
        }

        builder.AppendLine();
        builder.Append("Current user query: ").Append(userQuery);
        return builder.ToString();
    }

    private static int ReadBoundedInt(IConfiguration configuration, string key, int fallback, int min, int max) =>
        int.TryParse(configuration[key], NumberStyles.Integer, CultureInfo.InvariantCulture, out var value)
            ? Math.Clamp(value, min, max)
            : fallback;

    private static double ReadBoundedDouble(IConfiguration configuration, string key, double fallback, double min, double max) =>
        double.TryParse(configuration[key], NumberStyles.Float, CultureInfo.InvariantCulture, out var value)
            ? Math.Clamp(value, min, max)
            : fallback;

    private static string ExtractJson(string content)
    {
        var fenced = Regex.Match(content, @"```(?:json)?\s*(\{[\s\S]*?\})\s*```", RegexOptions.IgnoreCase);
        if (fenced.Success)
            return fenced.Groups[1].Value;

        var firstBrace = content.IndexOf('{');
        var lastBrace = content.LastIndexOf('}');
        return firstBrace >= 0 && lastBrace > firstBrace
            ? content[firstBrace..(lastBrace + 1)]
            : content;
    }

    public static QueryIntent HeuristicFallback(string userQuery)
    {
        var text = userQuery.Trim();
        var lower = text.ToLowerInvariant();

        if (IsSmallTalk(lower))
        {
            return new QueryIntent
            {
                Kind = QueryIntentKinds.Chat,
                Confidence = 1.0,
                Reply = BuildChatReply(lower)
            };
        }

        var intent = new QueryIntent
        {
            Kind = QueryIntentKinds.Query,
            Intent = QueryIntentKinds.MetricQuery,
            Confidence = 0.45
        };

        if (lower.Contains("выруч", StringComparison.Ordinal) || lower.Contains("revenue", StringComparison.Ordinal) || lower.Contains("продаж", StringComparison.Ordinal) || lower.Contains("sales", StringComparison.Ordinal))
            intent.Metric = "revenue_sum";
        else if (lower.Contains("самых дорог", StringComparison.Ordinal) || lower.Contains("самые дорог", StringComparison.Ordinal) || lower.Contains("дорогих заказ", StringComparison.Ordinal))
        {
            intent.Metric = "order_price";
            intent.Dimensions.Add("order");
            intent.Visualization = "table";
        }
        else if (lower.Contains("средн", StringComparison.Ordinal) && lower.Contains("чек", StringComparison.Ordinal))
            intent.Metric = "avg_order_price";
        else if (lower.Contains("длитель", StringComparison.Ordinal) || lower.Contains("duration", StringComparison.Ordinal))
            intent.Metric = "avg_trip_duration";
        else if (lower.Contains("расстоя", StringComparison.Ordinal) || lower.Contains("distance", StringComparison.Ordinal) || lower.Contains("дистанц", StringComparison.Ordinal))
            intent.Metric = "avg_distance_km";
        else if ((lower.Contains("доля", StringComparison.Ordinal) || lower.Contains("процент", StringComparison.Ordinal)) && lower.Contains("отмен", StringComparison.Ordinal))
            intent.Metric = "cancellation_rate";
        else if (lower.Contains("тендер", StringComparison.Ordinal))
            intent.Metric = "tenders_per_order";
        else if (lower.Contains("заказ", StringComparison.Ordinal) || lower.Contains("поезд", StringComparison.Ordinal) || lower.Contains("rides", StringComparison.Ordinal))
            intent.Metric = "orders_count";

        if (lower.Contains("город", StringComparison.Ordinal))
            intent.Dimensions.Add("city");
        else if (lower.Contains("по дням", StringComparison.Ordinal) || lower.Contains("дата", StringComparison.Ordinal))
            intent.Dimensions.Add("day");
        else if (lower.Contains("недел", StringComparison.Ordinal))
            intent.Dimensions.Add("week");
        else if (lower.Contains("месяц", StringComparison.Ordinal))
            intent.Dimensions.Add("month");
        else if (lower.Contains("час", StringComparison.Ordinal))
            intent.Dimensions.Add("hour");
        else if (lower.Contains("статус", StringComparison.Ordinal))
            intent.Dimensions.Add("status_order");

        if (lower.Contains("отмен", StringComparison.Ordinal))
        {
            if (!string.Equals(intent.Metric, "cancellation_rate", StringComparison.OrdinalIgnoreCase))
            {
                intent.Metric ??= "orders_count";
                intent.Filters.Add(new IntentFilter
                {
                    Field = "status_order",
                    Operator = "=",
                    Value = "cancelled"
                });
            }
        }
        else if (lower.Contains("заверш", StringComparison.Ordinal) || lower.Contains("completed", StringComparison.Ordinal) || lower.Contains("done", StringComparison.Ordinal))
        {
            intent.Metric ??= "orders_count";
            intent.Filters.Add(new IntentFilter
            {
                Field = "status_order",
                Operator = "=",
                Value = "done"
            });
        }

        if (TryExtractLegacyComparisonPeriods(lower, out var periods))
        {
            intent.Intent = QueryIntentKinds.ComparePeriods;
            intent.Periods = periods;
            intent.Visualization = "bar";
        }
        else if (TryExtractDateRange(lower, out var dateRange))
        {
            intent.DateRange = dateRange;
        }

        AddPriceFilterFromText(intent, lower);

        var topMatch = Regex.Match(lower, @"(?:топ|top)\s*(\d{1,3})|^(?<leading>\d{1,3})\s+сам", RegexOptions.CultureInvariant);
        var topValue = topMatch.Success && int.TryParse(topMatch.Groups[1].Value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var topN)
            ? topN
            : topMatch.Success && int.TryParse(topMatch.Groups["leading"].Value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var leadingTopN)
                ? leadingTopN
                : (int?)null;
        if (topValue.HasValue)
        {
            intent.Limit = topValue.Value;
            intent.Sort.Add(new QuerySort
            {
                Field = intent.Metric ?? "metric",
                Direction = "desc"
            });
        }

        if (lower.Contains("по убыванию", StringComparison.OrdinalIgnoreCase) || lower.Contains("descending", StringComparison.OrdinalIgnoreCase))
        {
            intent.Sort.Add(new QuerySort
            {
                Field = intent.Metric ?? "metric",
                Direction = "desc"
            });
        }
        else if (lower.Contains("по возрастанию", StringComparison.OrdinalIgnoreCase) || lower.Contains("ascending", StringComparison.OrdinalIgnoreCase))
        {
            intent.Sort.Add(new QuerySort
            {
                Field = intent.Metric ?? "metric",
                Direction = "asc"
            });
        }

        intent.Visualization = intent.Visualization ?? InferVisualization(intent, lower);
        intent.Aggregation = intent.Metric switch
        {
            "revenue_sum" => "sum",
            "order_price" => "max",
            "avg_order_price" or "avg_trip_duration" or "avg_distance_km" or "tenders_per_order" => "avg",
            "cancellation_rate" => "formula",
            _ => "count"
        };

        var confidence = 0.2;
        if (!string.IsNullOrWhiteSpace(intent.Metric))
            confidence += 0.35;
        if (intent.Dimensions.Count > 0)
            confidence += 0.15;
        if (intent.DateRange != null || intent.Periods?.Count > 0)
            confidence += 0.15;
        if (intent.Filters.Count > 0)
            confidence += 0.1;
        if (intent.Limit.HasValue)
            confidence += 0.05;
        if (intent.Intent == QueryIntentKinds.ComparePeriods)
            confidence += 0.05;

        intent.Confidence = Math.Min(confidence, 0.9);
        intent.Explanation = BuildExplanation(intent);
        return intent;
    }

    private static void AddPriceFilterFromText(QueryIntent intent, string text)
    {
        if (intent.Filters.Any(filter => string.Equals(filter.Field, "price_order_local", StringComparison.OrdinalIgnoreCase)))
            return;

        var match = Regex.Match(
            text,
            @"(?:сумм\w*|цен\w*|стоимост\w*|чек\w*|руб\w*)\D{0,24}(?<operator>больше|более|выше|дороже|от|>=|>|меньше|менее|ниже|дешевле|до|<=|<)\D{0,12}(?<value>\d+(?:[\s\u00A0]\d{3})*(?:[,.]\d+)?)",
            RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);
        if (!match.Success)
            return;

        var valueText = match.Groups["value"].Value.Replace(" ", "", StringComparison.Ordinal).Replace("\u00A0", "", StringComparison.Ordinal);
        if (!decimal.TryParse(valueText.Replace(',', '.'), NumberStyles.Number, CultureInfo.InvariantCulture, out var value))
            return;

        var normalizedOperator = match.Groups["operator"].Value switch
        {
            "больше" or "более" or "выше" or "дороже" or "от" or ">" => ">",
            ">=" => ">=",
            "меньше" or "менее" or "ниже" or "дешевле" or "до" or "<" => "<",
            "<=" => "<=",
            _ => ">"
        };

        intent.Filters.Add(new IntentFilter
        {
            Field = "price_order_local",
            Operator = normalizedOperator,
            Value = value
        });
    }

    private static bool TryExtractLegacyComparisonPeriods(string text, out List<string>? periods)
    {
        periods = null;

        if (PeriodTextPatterns.MentionsCurrentAndPreviousYear(text))
        {
            periods = new List<string> { "current_year", "previous_year" };
            return true;
        }

        if (PeriodTextPatterns.MentionsCurrentAndPreviousMonth(text))
        {
            periods = new List<string> { "current_month", "previous_month" };
            return true;
        }

        if (PeriodTextPatterns.MentionsCurrentAndPreviousWeek(text))
        {
            periods = new List<string> { "current_week", "previous_week" };
            return true;
        }

        return false;
    }

    private static bool TryExtractDateRange(string text, out QueryDateRange? dateRange)
    {
        dateRange = null;

        var absolute = Regex.Match(
            text,
            @"(?:с|from)\s+(?<from>\d{4}-\d{2}-\d{2}|\d{2}\.\d{2}\.\d{4})\s+(?:по|to|-)\s+(?<to>\d{4}-\d{2}-\d{2}|\d{2}\.\d{2}\.\d{4})",
            RegexOptions.CultureInvariant);
        if (absolute.Success)
        {
            dateRange = new QueryDateRange
            {
                Type = "absolute",
                Start = absolute.Groups["from"].Value,
                End = absolute.Groups["to"].Value,
                DateColumn = SemanticLayer.DefaultDateColumn
            };
            return true;
        }

        var rolling = Regex.Match(
            text,
            @"(?:за|for|last|последн\w*\s+)?(?<value>\d{1,3})\s*(?<unit>дн(?:ей|я|ь)?|days?|недел(?:ь|и|ю)?|weeks?|месяц(?:а|ев)?|months?)",
            RegexOptions.CultureInvariant);
        if (rolling.Success && int.TryParse(rolling.Groups["value"].Value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var value) && value > 0)
        {
            var unit = rolling.Groups["unit"].Value;
            dateRange = new QueryDateRange
            {
                Type = unit.Contains("нед", StringComparison.Ordinal) || unit.Contains("week", StringComparison.Ordinal)
                    ? "last_n_weeks"
                    : unit.Contains("меся", StringComparison.Ordinal) || unit.Contains("month", StringComparison.Ordinal)
                        ? "last_n_months"
                        : "last_n_days",
                Value = value,
                DateColumn = SemanticLayer.DefaultDateColumn
            };
            return true;
        }

        if (text.Contains("вчера", StringComparison.OrdinalIgnoreCase) || text.Contains("yesterday", StringComparison.OrdinalIgnoreCase))
        {
            dateRange = new QueryDateRange { Type = "yesterday", DateColumn = SemanticLayer.DefaultDateColumn };
            return true;
        }

        if (text.Contains("сегодня", StringComparison.OrdinalIgnoreCase) || text.Contains("today", StringComparison.OrdinalIgnoreCase))
        {
            dateRange = new QueryDateRange { Type = "today", DateColumn = SemanticLayer.DefaultDateColumn };
            return true;
        }

        if (text.Contains("прошлая неделя", StringComparison.OrdinalIgnoreCase) || text.Contains("last week", StringComparison.OrdinalIgnoreCase))
        {
            dateRange = new QueryDateRange { Type = "previous_week", DateColumn = SemanticLayer.DefaultDateColumn };
            return true;
        }

        if (text.Contains("эта неделя", StringComparison.OrdinalIgnoreCase) || text.Contains("текущая неделя", StringComparison.OrdinalIgnoreCase) || text.Contains("this week", StringComparison.OrdinalIgnoreCase))
        {
            dateRange = new QueryDateRange { Type = "current_week", DateColumn = SemanticLayer.DefaultDateColumn };
            return true;
        }

        if (text.Contains("прошлый месяц", StringComparison.OrdinalIgnoreCase) || text.Contains("last month", StringComparison.OrdinalIgnoreCase))
        {
            dateRange = new QueryDateRange { Type = "previous_month", DateColumn = SemanticLayer.DefaultDateColumn };
            return true;
        }

        if (text.Contains("этот месяц", StringComparison.OrdinalIgnoreCase) || text.Contains("текущий месяц", StringComparison.OrdinalIgnoreCase) || text.Contains("this month", StringComparison.OrdinalIgnoreCase))
        {
            dateRange = new QueryDateRange { Type = "current_month", DateColumn = SemanticLayer.DefaultDateColumn };
            return true;
        }

        if (text.Contains("прошлый год", StringComparison.OrdinalIgnoreCase) || text.Contains("last year", StringComparison.OrdinalIgnoreCase))
        {
            dateRange = new QueryDateRange { Type = "previous_year", DateColumn = SemanticLayer.DefaultDateColumn };
            return true;
        }

        if (text.Contains("этот год", StringComparison.OrdinalIgnoreCase) || text.Contains("текущий год", StringComparison.OrdinalIgnoreCase) || text.Contains("this year", StringComparison.OrdinalIgnoreCase))
        {
            dateRange = new QueryDateRange { Type = "current_year", DateColumn = SemanticLayer.DefaultDateColumn };
            return true;
        }

        return false;
    }

    private static string InferVisualization(QueryIntent intent, string lowerText)
    {
        if (intent.Intent == QueryIntentKinds.ComparePeriods)
            return "bar";

        if (lowerText.Contains("доля", StringComparison.OrdinalIgnoreCase) || lowerText.Contains("pie", StringComparison.OrdinalIgnoreCase))
            return "pie";

        if (intent.Dimensions.Any(dimension => dimension is "day" or "week" or "month" or "hour"))
            return "line";

        if (intent.Dimensions.Count > 0)
            return "bar";

        return "table";
    }

    private static string BuildExplanation(QueryIntent intent)
    {
        var parts = new List<string>();
        if (!string.IsNullOrWhiteSpace(intent.Metric))
            parts.Add($"метрика = {intent.Metric}");
        if (intent.Dimensions.Count > 0)
            parts.Add($"группировка = {string.Join(", ", intent.Dimensions)}");
        if (intent.DateRange != null)
            parts.Add($"period = {intent.DateRange.Type}");
        if (intent.Filters.Count > 0)
            parts.Add($"filters = {string.Join(", ", intent.Filters.Select(filter => $"{filter.Field} {filter.Operator} {IntentValueHelper.ToDisplayString(filter.Value)}"))}");
        if (intent.Periods?.Count > 0)
            parts.Add($"periods = {string.Join(" vs ", intent.Periods)}");

        return parts.Count == 0
            ? "Heuristic parser did not extract a stable intent."
            : "Heuristic parser: " + string.Join("; ", parts) + ".";
    }

    private static bool IsSmallTalk(string lowerText)
    {
        var greetings = new[] { "привет", "здравств", "добрый день", "доброе утро", "добрый вечер", "hello", "hi" };
        var meta = new[] { "кто ты", "что ты умеешь", "что ты можешь", "помощь", "help", "как пользоваться", "как работать" };
        var thanks = new[] { "спасибо", "благодарю", "thanks", "thank you" };

        if (greetings.Any(lowerText.Contains))
            return true;
        if (meta.Any(lowerText.Contains))
            return true;
        return thanks.Any(lowerText.Contains) && lowerText.Length < 40;
    }

    private static string BuildChatReply(string lowerText)
    {
        if (lowerText.Contains("кто ты", StringComparison.OrdinalIgnoreCase) || lowerText.Contains("что ты умеешь", StringComparison.OrdinalIgnoreCase))
        {
            return "Я BI-ассистент Nexus Data Space. Помогаю перевести запрос на естественном языке в структурированный intent, затем система кодом строит безопасный SQL, выполняет его и показывает таблицу, график и объяснение.";
        }

        if (lowerText.Contains("help", StringComparison.OrdinalIgnoreCase) || lowerText.Contains("помощь", StringComparison.OrdinalIgnoreCase))
        {
            return "Спросите, например: «Количество заказов по дням за последние 5 дней», «Выручка по городам за прошлую неделю» или «Топ 3 города по отменённым заказам на этой неделе».";
        }

        return "Сформулируйте вопрос про заказы, выручку, отмены или средний чек, и я разберу его в аналитический intent.";
    }
}

public static class PromptTemplates
{
    public static string SystemPrompt(SemanticLayer semanticLayer)
    {
        if (!bool.TryParse(Environment.GetEnvironmentVariable("NEXUS_DATA_SPACE_USE_LEGACY_PROMPT"), out var useLegacyPrompt) || !useLegacyPrompt)
            return CompactPromptTemplates.SystemPrompt(semanticLayer);

        var metrics = string.Join(Environment.NewLine, semanticLayer.Metrics.Select(metric =>
            $"- {metric.Key}: {metric.DisplayLabel}; aggregation={metric.Aggregation}; source={metric.Source}; date_column={metric.DateColumn}; synonyms=[{string.Join(", ", metric.Synonyms.Take(8))}]"));
        var dimensions = string.Join(Environment.NewLine, semanticLayer.Dimensions.Select(dimension =>
            $"- {dimension.Key}: {dimension.DisplayLabel}; source={dimension.Source}; synonyms=[{string.Join(", ", dimension.Synonyms.Take(6))}]"));
        var filters = string.Join(Environment.NewLine, semanticLayer.Filters.Select(filter =>
            $"- {filter.Key}: {filter.DisplayLabel}; operators=[{string.Join(", ", filter.AllowedOperators)}]; synonyms=[{string.Join(", ", filter.Synonyms.Take(6))}]"));
        var sources = string.Join(Environment.NewLine, semanticLayer.Sources.Select(source =>
            $"- {source.Key}: table={source.Table}; columns=[{string.Join(", ", source.AllowedColumns.Take(40))}]"));
        var presets = semanticLayer.Presets.Count == 0
            ? "- нет специальных presets; используй только метрики, dimensions и filters выше."
            : string.Join(Environment.NewLine, semanticLayer.Presets.Select(preset =>
                $"- если запрос похож на [{string.Join(", ", preset.Phrases.Take(5))}], metric={preset.Metric ?? "не менять"}, dimension={preset.Dimension ?? "не менять"}, filters=[{string.Join(", ", preset.Filters.Select(filter => $"{filter.Field} {filter.Operator} {IntentValueHelper.ToDisplayString(filter.Value)}"))}]"));
        var dimensionKeys = string.Join("\" | \"", semanticLayer.Dimensions.Select(dimension => dimension.Key));
        var filterKeys = string.Join("\" | \"", semanticLayer.Filters.Select(filter => filter.Key));
        var sourceKeys = string.Join("\" | \"", semanticLayer.Sources.Select(source => source.Key));
        var dateColumns = string.Join("\" | \"", semanticLayer.Metrics.Select(metric => metric.DateColumn).Distinct(StringComparer.OrdinalIgnoreCase));
        var sortFields = string.Join("\" | \"", new[] { "metric", "period" }.Concat(semanticLayer.Dimensions.Select(dimension => dimension.Key)).Distinct(StringComparer.OrdinalIgnoreCase));
        return $@"Ты — NL parser для BI-сервиса Nexus Data Space. Твоя задача: извлечь смысл пользовательского запроса и вернуть только JSON intent.

ВАЖНО:
- НИКОГДА не пиши финальный SQL.
- НИКОГДА не придумывай таблицы или поля вне перечисленного semantic layer.
- Возвращай только один JSON-объект без markdown, без комментариев, без текста до и после.
- Если запрос не аналитический — верни kind=""chat"".
- Если данных для безопасного SQL недостаточно — верни kind=""clarify"" и короткое clarification/reply.

Доступные метрики:
{metrics}

Доступные dimensions:
{dimensions}

Доступные filters:
{filters}

Доступные sources/tables:
{sources}

Business presets:
{presets}

Разрешённая схема:
{{
  ""kind"": ""query"" | ""chat"" | ""clarify"",
  ""reply"": string | null,
  ""clarification"": string | null,
  ""intent"": ""metric_query"" | ""compare_periods"",
  ""metric"": string | null,
  ""aggregation"": ""count"" | ""sum"" | ""avg"" | ""max"" | ""formula"" | null,
  ""dimensions"": [""{dimensionKeys}""],
  ""filters"": [
    {{
      ""field"": ""{filterKeys}"",
      ""operator"": ""="" | ""!="" | ""in"" | ""not_in"" | "">"" | "">="" | ""<"" | ""<="",
      ""value"": string | number | [string]
    }}
  ],
  ""date_range"": {{
    ""type"": ""today"" | ""yesterday"" | ""last_n_days"" | ""last_n_weeks"" | ""last_n_months"" | ""current_week"" | ""previous_week"" | ""current_month"" | ""previous_month"" | ""current_year"" | ""previous_year"" | ""absolute"",
    ""value"": number | null,
    ""start"": ""YYYY-MM-DD"" | null,
    ""end"": ""YYYY-MM-DD"" | null,
    ""date_column"": ""{dateColumns}"" | null
  }} | null,
  ""sort"": [
    {{
      ""field"": ""{sortFields}"",
      ""direction"": ""asc"" | ""desc""
    }}
  ],
  ""limit"": number | null,
  ""source"": ""{sourceKeys}"" | null,
  ""comparison"": {{
    ""mode"": ""period_vs_period"",
    ""periods"": [{{ ""type"": ""..."", ""start"": ""..."", ""end"": ""..."" }}, {{ ""type"": ""..."", ""start"": ""..."", ""end"": ""..."" }}]
  }} | null,
  ""confidence"": number,
  ""explanation"": string | null
}}

Дополнительные правила:
- Use only metric/dimension/filter/source keys from the semantic layer above.
- If the business meaning is covered by Business presets, apply the matching metric, dimensions and filters.
- Do not transfer Nexus Data Space-specific terms to another database unless those terms are explicitly present in semantic layer.
- Для временного ряда используй visualization = line.
- Для top N обычно нужен limit и sort по metric desc.
- Для compare_periods можно заполнить periods или comparison.periods.
- Если пользователь пишет «последняя неделя», «последний месяц» или «последний год» без уточнения, это неоднозначно: верни kind=""clarify"" и спроси, нужен прошлый календарный период или последние N дней/месяцев.
- Если не уверен — лучше верни kind=""clarify"", чем рискованный query intent.";
    }
}
