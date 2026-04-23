using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using DriveeDataSpace.Web.Models;

namespace DriveeDataSpace.Web.Services;

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

    public async Task<QueryIntent> InterpretAsync(string userQuery, string systemPrompt, CancellationToken ct = default)
    {
        var payload = new
        {
            model = _model,
            temperature = 0.1,
            messages = new object[]
            {
                new { role = "system", content = systemPrompt },
                new { role = "user",   content = userQuery }
            }
        };

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
            return HeuristicFallback(userQuery);
        }

        var jsonText = ExtractJson(content);
        try
        {
            var intent = JsonSerializer.Deserialize<QueryIntent>(jsonText, new JsonSerializerOptions
            {
                PropertyNameCaseInsensitive = true
            });
            return intent ?? HeuristicFallback(userQuery);
        }
        catch (JsonException)
        {
            _log.LogWarning("LLM returned non-JSON: {C}", content);
            var fb = HeuristicFallback(userQuery);
            fb.Explanation = "LLM вернул неразбираемый ответ. Использована эвристика.";
            return fb;
        }
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

    public static QueryIntent HeuristicFallback(string q)
    {
        var lower = q.ToLowerInvariant();
        var intent = new QueryIntent { Confidence = 0.35, Explanation = "LLM недоступен — использована эвристика по ключевым словам." };

        if (IsSmallTalk(lower))
        {
            intent.Kind = "chat";
            intent.Confidence = 0.9;
            intent.Reply = BuildChatReply(lower);
            return intent;
        }

        if (lower.Contains("выручк") || lower.Contains("revenue") || lower.Contains("доход")) intent.Metric = "revenue";
        else if (lower.Contains("средн") && lower.Contains("чек")) intent.Metric = "avg_check";
        else if (lower.Contains("длительност") || lower.Contains("duration") || lower.Contains("продолжит")) intent.Metric = "duration";
        else if (lower.Contains("расстоян") || lower.Contains("дистанц") || lower.Contains("distance") || lower.Contains(" км")) intent.Metric = "distance";
        else if ((lower.Contains("доля") || lower.Contains("процент")) && lower.Contains("отмен")) intent.Metric = "cancellation_rate";
        else if (lower.Contains("отмен") || lower.Contains("cancel")) intent.Metric = "cancellations";
        else if (lower.Contains("тендер")) intent.Metric = "tenders_per_order";
        else intent.Metric = "orders";

        if (lower.Contains("город")) intent.GroupBy = "city";
        else if (lower.Contains("час")) intent.GroupBy = "hour";
        else if (lower.Contains("месяц")) intent.GroupBy = "month";
        else if (lower.Contains("недел")) intent.GroupBy = "week";
        else if (lower.Contains("статус")) intent.GroupBy = "status";
        else if (lower.Contains("дн") || lower.Contains("дат")) intent.GroupBy = "day";

        if (lower.Contains("вчера")) intent.Period = "yesterday";
        else if (lower.Contains("сегодня")) intent.Period = "today";
        else if (lower.Contains("прошл") && lower.Contains("недел")) intent.Period = "last_week";
        else if (lower.Contains("прошл") && lower.Contains("месяц")) intent.Period = "last_month";
        else if (lower.Contains("прошл") && lower.Contains("год")) intent.Period = "previous_year";
        else if (lower.Contains("текущ") && lower.Contains("год")) intent.Period = "current_year";
        else if (lower.Contains("7 дн") || lower.Contains("семь дн")) intent.Period = "last_7_days";
        else if (lower.Contains("30 дн") || lower.Contains("месяц")) intent.Period = "last_30_days";

        if (lower.Contains("сравн"))
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

        return intent;
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
        return $@"Ты — BI-ассистент Drivee (база заказов такси). Анализируешь сообщение пользователя и ВСЕГДА возвращаешь СТРОГО один JSON-объект без пояснений, без ```, без текста до/после.

Сначала определи kind:
- ""query"" — пользователь хочет получить данные (метрика, группировка, период, фильтр, сравнение).
- ""chat"" — приветствие, благодарность, вопрос о возможностях бота, small talk. НЕ пытайся искать метрику. Заполни reply дружелюбным ответом на русском (2-4 предложения). Можно кратко перечислить, что ты умеешь, и привести пример запроса.
- ""clarify"" — запрос похож на аналитический, но чего-то не хватает (нет метрики/периода/группировки). Заполни reply уточняющим вопросом.

Схема:
{{
  ""kind"": ""query"" | ""chat"" | ""clarify"",
  ""reply"": null | string,
  ""intent"": ""aggregate"" | ""compare_periods"",
  ""metric"": одно из [{metrics}],
  ""group_by"": null | одно из [{dims}],
  ""period"": null | ""today"" | ""yesterday"" | ""last_week"" | ""current_week"" | ""last_month"" | ""current_month"" | ""current_year"" | ""previous_year"" | ""last_7_days"" | ""last_30_days"",
  ""periods"": null | массив из двух периодов (для intent=compare_periods),
  ""filters"": null | {{""city"":""..."", ""status"":""...""}},
  ""visualization_hint"": ""bar"" | ""line"" | ""pie"" | ""table"",
  ""confidence"": 0.0..1.0,
  ""explanation"": null | короткое пояснение на русском, как ты понял запрос (для kind=query)
}}

Правила:
- Если kind=chat или clarify — поля metric/period и прочие можно оставить дефолтами, они не используются; confidence = 1.0.
- Временной ряд (day/week/month) → visualization_hint=line.
- Сравнение категорий (hour/status) → bar.
- Без группировки → table.
- Если метрика/период неясны — kind=clarify и задай вопрос в reply.";
    }
}
