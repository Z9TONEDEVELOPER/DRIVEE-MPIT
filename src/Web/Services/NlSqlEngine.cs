using System.Text.Json;
using DriveeDataSpace.Web.Models;
using Microsoft.Extensions.Logging;

namespace DriveeDataSpace.Web.Services;

public class NlSqlEngine
{
    private readonly LlmService _llm;
    private readonly SemanticLayer _semantic;
    private readonly SqlBuilder _builder;
    private readonly QueryExecutor _executor;
    private readonly ILogger<NlSqlEngine> _log;

    public NlSqlEngine(LlmService llm, SemanticLayer semantic, SqlBuilder builder, QueryExecutor executor, ILogger<NlSqlEngine> log)
    {
        _llm = llm;
        _semantic = semantic;
        _builder = builder;
        _executor = executor;
        _log = log;
    }

    public async Task<PipelineResult> RunAsync(
        string userQuery,
        IReadOnlyList<ChatTurn>? history = null,
        QueryIntent? previousIntent = null,
        CancellationToken ct = default)
    {
        var pr = new PipelineResult { UserQuery = userQuery };
        try
        {
            var system = PromptTemplates.SystemPrompt(_semantic);
            var intent = await _llm.InterpretAsync(userQuery, system, history, previousIntent, ct);
            pr.Intent = intent;
            pr.Confidence = intent.Confidence;
            pr.Visualization = intent.VisualizationHint;

            if (intent.Kind == "chat" || intent.Kind == "clarify")
            {
                pr.IsChat = true;
                pr.ChatReply = !string.IsNullOrWhiteSpace(intent.Reply)
                    ? intent.Reply
                    : "Я BI-ассистент Drivee. Спросите про заказы, выручку или отмены — например: «Выручка по месяцам».";
                return pr;
            }

            if (_semantic.ResolveMetric(intent.Metric) == null)
            {
                pr.Error = $"Метрика «{intent.Metric}» не найдена в семантическом слое.";
                return pr;
            }

            if (intent.Confidence < 0.4)
                pr.Warnings.Add($"Низкая уверенность ({intent.Confidence:P0}). Уточните запрос.");

            var built = _builder.Build(intent, userQuery);
            pr.Sql = FormatSqlForDisplay(built.Sql, built.Parameters);
            pr.Explain = built.HumanExplain;
            pr.TechnicalExplain = built.TechExplain;
            pr.ReasoningTrail = built.ReasoningTrail;
            pr.ReasoningTrail.Add(new Models.ReasoningStep(
                "⚙️",
                "SQL-запрос",
                "Итоговый безопасный SELECT (read-only, параметризованный, с LIMIT):",
                pr.Sql));
            pr.ReasoningTrail.Add(new Models.ReasoningStep(
                "❓",
                "Я правильно понял?",
                "Если какая-то деталь (метрика, период, группировка) разошлась с вашим намерением — напишите уточнение в чат, и я пересоберу запрос."));

            pr.Result = _executor.Execute(built.Sql, built.Parameters);
            if (pr.Result.RowCount == 0) pr.Warnings.Add("Запрос выполнен, но данных нет.");
        }
        catch (Exception ex)
        {
            _log.LogError(ex, "Pipeline failed");
            pr.Error = ex.Message;
        }
        return pr;
    }

    public PipelineResult ReplayFromReport(string intentJson, CancellationToken ct = default)
    {
        var pr = new PipelineResult();
        var intent = JsonSerializer.Deserialize<QueryIntent>(intentJson)
                     ?? throw new InvalidOperationException("Bad report intent JSON");
        pr.Intent = intent;
        pr.Confidence = intent.Confidence;
        pr.Visualization = intent.VisualizationHint;

        var built = _builder.Build(intent, null);
        pr.Sql = FormatSqlForDisplay(built.Sql, built.Parameters);
        pr.Explain = built.HumanExplain + " (повторный запуск)";
        pr.TechnicalExplain = built.TechExplain;
        pr.ReasoningTrail = built.ReasoningTrail;
        pr.ReasoningTrail.Add(new Models.ReasoningStep(
            "⚙️",
            "SQL-запрос",
            "Итоговый SELECT (повторный запуск сохранённого отчёта):",
            pr.Sql));
        pr.Result = _executor.Execute(built.Sql, built.Parameters);
        return pr;
    }

    private static string FormatSqlForDisplay(string sql, Dictionary<string, object?> pars)
    {
        var display = sql;
        foreach (var kv in pars.OrderByDescending(k => k.Key.Length))
        {
            var v = kv.Value switch
            {
                null => "NULL",
                string s => $"'{s.Replace("'", "''")}'",
                _ => kv.Value.ToString() ?? "NULL"
            };
            display = display.Replace(kv.Key, v);
        }
        return display;
    }
}
