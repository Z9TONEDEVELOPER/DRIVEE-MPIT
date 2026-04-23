using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using DriveeDataSpace.Web.Models;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Http;
using Microsoft.Extensions.Logging;

namespace DriveeDataSpace.Web.Services;

public record ChatTurn(string Role, string Content);

public class LlmService
{
    private readonly HttpClient _http;
    private readonly ILogger<LlmService> _log;
    private readonly string _endpoint;
    private readonly string _model;

    public LlmService(IHttpClientFactory factory, IConfiguration cfg, ILogger<LlmService> log)
    {
        _http = factory.CreateClient("llm");
        _log = log;
        _endpoint = cfg["Llm:Endpoint"] ?? "http://localhost:1234/v1/chat/completions";
        _model = cfg["Llm:Model"] ?? "local-model";
        var timeout = int.TryParse(cfg["Llm:TimeoutSeconds"], out var t) ? t : 60;
        _http.Timeout = TimeSpan.FromSeconds(timeout);
    }

    public async Task<QueryIntent> InterpretAsync(
        string userQuery,
        string systemPrompt,
        IReadOnlyList<ChatTurn>? history = null,
        QueryIntent? previousIntent = null,
        CancellationToken ct = default)
    {
        var messages = new List<object> { new { role = "system", content = systemPrompt } };
        if (history != null)
        {
            foreach (var t in history.TakeLast(8))
                messages.Add(new { role = t.Role, content = t.Content });
        }
        messages.Add(new { role = "user", content = userQuery });

        var payload = new { model = _model, temperature = 0.1, messages = messages.ToArray() };

        var json = JsonSerializer.Serialize(payload);
        using var req = new HttpRequestMessage(HttpMethod.Post, _endpoint)
        {
            Content = new StringContent(json, Encoding.UTF8, "application/json")
        };

        string content;
        try
        {
            using var resp = await _http.SendAsync(req, ct);
            var body = await resp.Content.ReadAsStringAsync(ct);
            if (!resp.IsSuccessStatusCode)
                throw new InvalidOperationException($"LLM HTTP {(int)resp.StatusCode}: {body}");
            content = ExtractContent(body);
        }
        catch (Exception ex) when (ex is HttpRequestException or TaskCanceledException)
        {
            _log.LogWarning(ex, "LLM unreachable, falling back to heuristic");
            return HeuristicFallback(userQuery, previousIntent);
        }

        var jsonText = ExtractJson(content);
        try
        {
            var intent = JsonSerializer.Deserialize<QueryIntent>(jsonText, new JsonSerializerOptions
            {
                PropertyNameCaseInsensitive = true
            });
            if (intent == null) return HeuristicFallback(userQuery, previousIntent);
            MergeWithPrevious(intent, previousIntent);
            ApplyExplicitPeriodHints(intent, userQuery);
            return intent;
        }
        catch (JsonException)
        {
            _log.LogWarning("LLM returned non-JSON: {C}", content);
            var fb = HeuristicFallback(userQuery, previousIntent);
            fb.Explanation = "LLM вернул неразбираемый ответ. Использована эвристика.";
            return fb;
        }
    }

    private static void MergeWithPrevious(QueryIntent current, QueryIntent? prev)
    {
        if (prev == null || prev.Kind != "query") return;
        if (current.Kind != "query") return;
        // If current looks like a refinement (missing period/group but same metric), inherit
        if (string.IsNullOrEmpty(current.Period) && !string.IsNullOrEmpty(prev.Period))
            current.Period = prev.Period;
        if (string.IsNullOrEmpty(current.GroupBy) && !string.IsNullOrEmpty(prev.GroupBy))
            current.GroupBy = prev.GroupBy;
        if (current.Filters == null && prev.Filters != null)
            current.Filters = prev.Filters;
    }

    private static void ApplyExplicitPeriodHints(QueryIntent intent, string userQuery)
    {
        if (intent.Kind != "query") return;

        if (TryExtractRollingDaysPeriod(userQuery, out var rollingPeriod))
            intent.Period = rollingPeriod;
    }

    private static bool TryExtractRollingDaysPeriod(string query, out string? period)
    {
        var lower = query.ToLowerInvariant();
        var match = Regex.Match(
            lower,
            @"(?:(?:за|for|last)\s+)?(?:последн\w*\s+)?(\d{1,3})\s*(?:дн(?:ей|я|ь)?|days?)",
            RegexOptions.CultureInvariant);

        if (!match.Success || !int.TryParse(match.Groups[1].Value, out var days) || days <= 0)
        {
            period = null;
            return false;
        }

        var matchedText = match.Value;
        var hasPeriodCue = matchedText.Contains("за") || matchedText.Contains("послед") || matchedText.Contains("last") || matchedText.Contains("for");
        if (!hasPeriodCue)
        {
            period = null;
            return false;
        }

        period = $"last_{days}_days";
        return true;
    }

    private static string ExtractContent(string body)
    {
        using var doc = JsonDocument.Parse(body);
        var root = doc.RootElement;
        if (root.TryGetProperty("choices", out var choices) && choices.ValueKind == JsonValueKind.Array && choices.GetArrayLength() > 0)
        {
            var first = choices[0];
            if (first.TryGetProperty("message", out var msg) && msg.TryGetProperty("content", out var c))
                return c.GetString() ?? "";
            if (first.TryGetProperty("text", out var tx))
                return tx.GetString() ?? "";
        }
        if (root.TryGetProperty("output", out var o) && o.ValueKind == JsonValueKind.String)
            return o.GetString() ?? "";
        if (root.TryGetProperty("response", out var r) && r.ValueKind == JsonValueKind.String)
            return r.GetString() ?? "";
        return body;
    }

    private static string ExtractJson(string content)
    {
        var fence = Regex.Match(content, @"```(?:json)?\s*(\{[\s\S]*?\})\s*```", RegexOptions.IgnoreCase);
        if (fence.Success) return fence.Groups[1].Value;

        var first = content.IndexOf('{');
        var last = content.LastIndexOf('}');
        if (first >= 0 && last > first) return content.Substring(first, last - first + 1);
        return content;
    }

    public static QueryIntent HeuristicFallback(string q, QueryIntent? previous = null)
    {
        var lower = q.ToLowerInvariant();
        var intent = new QueryIntent();

        if (IsSmallTalk(lower))
        {
            intent.Kind = "chat";
            intent.Confidence = 0.9;
            intent.Reply = BuildChatReply(lower);
            return intent;
        }

        bool metricMatched = true;
        if (lower.Contains("выручк") || lower.Contains("revenue") || lower.Contains("доход")) intent.Metric = "revenue";
        else if (lower.Contains("средн") && lower.Contains("чек")) intent.Metric = "avg_check";
        else if (lower.Contains("длительност") || lower.Contains("duration") || lower.Contains("продолжит")) intent.Metric = "duration";
        else if (lower.Contains("расстоян") || lower.Contains("дистанц") || lower.Contains("distance") || lower.Contains(" км")) intent.Metric = "distance";
        else if ((lower.Contains("доля") || lower.Contains("процент")) && lower.Contains("отмен")) intent.Metric = "cancellation_rate";
        else if (lower.Contains("отмен") || lower.Contains("cancel")) intent.Metric = "cancellations";
        else if (lower.Contains("тендер")) intent.Metric = "tenders_per_order";
        else if (lower.Contains("заказ") || lower.Contains("поездк") || lower.Contains("order")) intent.Metric = "orders";
        else { intent.Metric = "orders"; metricMatched = false; }

        bool groupMatched = true;
        if (lower.Contains("город")) intent.GroupBy = "city";
        else if (lower.Contains("час")) intent.GroupBy = "hour";
        else if (lower.Contains("месяц")) intent.GroupBy = "month";
        else if (lower.Contains("недел")) intent.GroupBy = "week";
        else if (lower.Contains("статус")) intent.GroupBy = "status";
        else if (lower.Contains(" дн") || lower.Contains("дат") || lower.StartsWith("дн")) intent.GroupBy = "day";
        else groupMatched = false;

        bool periodMatched = true;
        if (lower.Contains("вчера")) intent.Period = "yesterday";
        else if (lower.Contains("сегодня")) intent.Period = "today";
        else if (lower.Contains("прошл") && lower.Contains("недел")) intent.Period = "last_week";
        else if (lower.Contains("прошл") && lower.Contains("месяц")) intent.Period = "last_month";
        else if (lower.Contains("прошл") && lower.Contains("год")) intent.Period = "previous_year";
        else if (lower.Contains("текущ") && lower.Contains("год")) intent.Period = "current_year";
        else if (lower.Contains("7 дн") || lower.Contains("семь дн")) intent.Period = "last_7_days";
        else if (lower.Contains("30 дн")) intent.Period = "last_30_days";
        else periodMatched = false;

        if (TryExtractRollingDaysPeriod(q, out var rollingPeriod))
        {
            intent.Period = rollingPeriod;
            periodMatched = true;
        }

        bool isCompare = lower.Contains("сравн");
        if (isCompare)
        {
            intent.Intent = "compare_periods";
            intent.Periods = new List<string> { "current_year", "previous_year" };
            intent.VisualizationHint = "bar";
        }
        else
        {
            intent.Intent = "aggregate";
            intent.VisualizationHint = intent.GroupBy switch
            {
                "day" or "week" or "month" => "line",
                "city" or "channel"         => "bar",
                null                         => "table",
                _                            => "bar"
            };
        }

        // Merge with previous context if this looks like a refinement
        if (previous != null && previous.Kind == "query")
        {
            if (!metricMatched) { intent.Metric = previous.Metric; metricMatched = true; }
            if (!groupMatched && !string.IsNullOrEmpty(previous.GroupBy)) { intent.GroupBy = previous.GroupBy; groupMatched = true; }
            if (!periodMatched && !string.IsNullOrEmpty(previous.Period)) { intent.Period = previous.Period; periodMatched = true; }
        }

        // Dynamic confidence: sum up signals actually detected
        double confidence = 0.25;
        if (metricMatched) confidence += 0.35;
        if (groupMatched) confidence += 0.20;
        if (periodMatched) confidence += 0.15;
        if (isCompare) confidence += 0.05;
        if (q.Trim().Length < 5) confidence *= 0.6;

        intent.Confidence = Math.Min(Math.Round(confidence, 2), 0.95);
        intent.Explanation = BuildHeuristicExplanation(intent, metricMatched, groupMatched, periodMatched);
        return intent;
    }

    private static string BuildHeuristicExplanation(QueryIntent i, bool m, bool g, bool p)
    {
        var parts = new List<string>();
        parts.Add(m ? $"метрика «{i.Metric}» распознана" : $"метрика по умолчанию — «{i.Metric}»");
        if (g) parts.Add($"группировка — {i.GroupBy}");
        if (p) parts.Add($"период — {i.Period}");
        if (!p && !g) parts.Add("группировка и период не указаны");
        return "Локальная эвристика: " + string.Join(", ", parts) + ".";
    }

    private static bool IsSmallTalk(string lower)
    {
        string[] greetings = { "привет", "здравствуй", "здравствуйте", "добрый день", "доброе утро", "добрый вечер", "hello", "hi ", "хай", "ку " };
        string[] meta = { "кто ты", "что ты умеешь", "что ты можешь", "что умеешь", "что можешь", "расскажи о себе", "о себе", "помощь", "help", "как пользоваться", "как работать", "что это", "для чего ты", "как начать" };
        string[] thanks = { "спасибо", "благодарю", "thanks", "thank you" };

        if (greetings.Any(lower.Contains)) return true;
        if (meta.Any(lower.Contains)) return true;
        if (thanks.Any(lower.Contains) && lower.Length < 40) return true;

        string[] dataKw =
        {
            "поездк","заказ","выручк","отмен","средн","длительн","расстоян","дистанц","тендер","цен","чек",
            "покаж","сколько","сравн","выведи","за ","по город","по дн","по месяц","по часам","по недел",
            "динамик","распределен","топ ","count","sum","avg"
        };
        return !dataKw.Any(lower.Contains);
    }

    private static string BuildChatReply(string lower)
    {
        if (lower.Contains("кто ты") || lower.Contains("о себе") || lower.Contains("что умеешь") || lower.Contains("что можешь") || lower.Contains("для чего"))
            return "Я — BI-ассистент Drivee. Помогаю получать данные из базы заказов без SQL: просто задайте вопрос на русском языке. " +
                   "Я умею считать метрики (заказы, выручка, средний чек, длительность, расстояние, отмены, доля отмен, тендеры), " +
                   "группировать их по дням / неделям / месяцам / часам / статусам, сравнивать периоды и сохранять отчёты для повторного запуска.\n\n" +
                   "Попробуйте: «Количество заказов по дням за последние 30 дней», «Сравни выручку за этот и прошлый месяц», «Распределение заказов по часам».";

        if (lower.Contains("как пользоваться") || lower.Contains("как начать") || lower.Contains("help") || lower.Contains("помощь"))
            return "Задайте вопрос в поле внизу — например, «Выручка по месяцам». Я покажу: как поняла запрос, итоговый SQL, таблицу и график. " +
                   "Любой результат можно сохранить как отчёт (кнопка 💾) и позже повторно запустить из боковой панели.";

        if (lower.Contains("спасибо") || lower.Contains("благодар") || lower.Contains("thank"))
            return "Всегда пожалуйста! Если нужен ещё какой-то срез данных — пишите.";

        return "Привет! Я BI-ассистент Drivee. Задайте вопрос про заказы, выручку или отмены — например: «Количество заказов по дням за последнюю неделю».";
    }
}

public static class PromptTemplates
{
    public static string SystemPrompt(SemanticLayer s)
    {
        var metrics = string.Join(", ", s.Metrics.Select(m => $"{m.Key} ({string.Join("/", m.Synonyms.Take(2))})"));
        var dims = string.Join(", ", s.Dimensions.Select(d => $"{d.Key} ({string.Join("/", d.Synonyms.Take(2))})"));
        return $@"Ты — BI-ассистент Drivee (база заказов такси). Работаешь в формате многошагового диалога: учитывай предыдущие сообщения как контекст. ВСЕГДА возвращай СТРОГО один JSON-объект без пояснений, без ```, без текста до/после.

Сначала определи kind:
- ""query"" — пользователь хочет получить данные (метрика, группировка, период, фильтр, сравнение). Это может быть ПЕРВЫЙ запрос ИЛИ УТОЧНЕНИЕ предыдущего (в последнем случае возьми недостающие поля из предыдущего intent в истории и дополни новыми).
- ""chat"" — приветствие, благодарность, вопрос о возможностях бота, small talk. НЕ пытайся искать метрику. Заполни reply дружелюбным ответом на русском (2-4 предложения).
- ""clarify"" — запрос похож на аналитический, но чего-то не хватает (нет метрики/периода/группировки) И в истории нет достаточного контекста. Заполни reply уточняющим вопросом.

Схема:
{{
  ""kind"": ""query"" | ""chat"" | ""clarify"",
  ""reply"": null | string,
  ""intent"": ""aggregate"" | ""compare_periods"",
  ""metric"": одно из [{metrics}],
  ""group_by"": null | одно из [{dims}],
  ""period"": null | ""today"" | ""yesterday"" | ""last_week"" | ""current_week"" | ""last_month"" | ""current_month"" | ""current_year"" | ""previous_year"" | ""last_7_days"" | ""last_30_days"" | ""last_N_days"",
  ""periods"": null | массив из двух периодов (для intent=compare_periods),
  ""filters"": null | {{""city"":""..."", ""status"":""...""}},
  ""visualization_hint"": ""bar"" | ""line"" | ""pie"" | ""table"",
  ""confidence"": 0.0..1.0,
  ""explanation"": null | короткое пояснение на русском, как ты понял запрос (для kind=query)
}}

Правила:
- Если в истории уже есть query-intent и пользователь пишет уточнение типа «а теперь по городам», «за последний месяц», «сравни с прошлым годом» — это kind=query, скопируй metric/group_by/period из предыдущего intent и перезапиши только то, что уточнил пользователь. confidence должен быть высоким (≥0.8).
- Если kind=chat или clarify — поля metric/period и прочие можно оставить дефолтами. confidence = 1.0 для chat, 0.3 для clarify.
- Временной ряд (day/week/month) → visualization_hint=line. Сравнение категорий (hour/status) → bar. Без группировки → table.
- confidence для query: 0.9 если распознаны и метрика, и период, и группировка; 0.7 если двое из трёх; 0.5 если только метрика; ниже 0.4 — если что-то очень неуверенно распознано.";
    }
}
