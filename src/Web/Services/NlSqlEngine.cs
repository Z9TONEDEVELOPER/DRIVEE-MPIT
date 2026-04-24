using System.Text.Json;
using System.Text.RegularExpressions;
using DriveeDataSpace.Web.Models;
using Microsoft.Extensions.Logging;

namespace DriveeDataSpace.Web.Services;

public class NlSqlEngine
{
    private static readonly Regex WriteIntentPattern = new(
        @"\b(drop|delete|update|insert|alter|create|truncate|replace|grant|revoke)\b|(?:^|\s)(удали|удалить|удалите|обнови|обновить|обновите|измени|изменить|измените|создай|создать|создайте|добавь|добавить|добавьте|вставь|вставить|вставьте|очисти|очистить|очистите)\b",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Compiled);

    private readonly LlmService _llmService;
    private readonly SemanticLayer _semanticLayer;
    private readonly IntentValidator _intentValidator;
    private readonly SqlBuilder _sqlBuilder;
    private readonly ExplainEngine _explainEngine;
    private readonly QueryExecutor _queryExecutor;
    private readonly ILogger<NlSqlEngine> _logger;

    public NlSqlEngine(
        LlmService llmService,
        SemanticLayer semanticLayer,
        IntentValidator intentValidator,
        SqlBuilder sqlBuilder,
        ExplainEngine explainEngine,
        QueryExecutor queryExecutor,
        ILogger<NlSqlEngine> logger)
    {
        _llmService = llmService;
        _semanticLayer = semanticLayer;
        _intentValidator = intentValidator;
        _sqlBuilder = sqlBuilder;
        _explainEngine = explainEngine;
        _queryExecutor = queryExecutor;
        _logger = logger;
    }

    public async Task<PipelineResult> RunAsync(
        string userQuery,
        IReadOnlyList<ChatTurn>? history = null,
        QueryIntent? previousIntent = null,
        CancellationToken cancellationToken = default)
    {
        var pipelineResult = new PipelineResult { UserQuery = userQuery };

        try
        {
            if (RequiresWriteAccess(userQuery))
            {
                pipelineResult.IsChat = true;
                pipelineResult.ChatReply = "Извините, доступ открыт только на чтение";
                pipelineResult.Confidence = 1.0;
                pipelineResult.Intent = new QueryIntent
                {
                    Kind = QueryIntentKinds.Chat,
                    Reply = pipelineResult.ChatReply,
                    Confidence = 1.0
                };
                return pipelineResult;
            }

            var rawIntent = await _llmService.InterpretAsync(
                userQuery,
                PromptTemplates.SystemPrompt(_semanticLayer),
                history,
                cancellationToken);

            var validation = _intentValidator.ValidateParsedIntent(rawIntent, userQuery, previousIntent);
            pipelineResult.Intent = validation.NormalizedIntent;
            pipelineResult.Confidence = validation.NormalizedIntent.Confidence;
            pipelineResult.Visualization = validation.NormalizedIntent.Visualization ?? validation.NormalizedIntent.VisualizationHint ?? "table";
            pipelineResult.Warnings.AddRange(validation.Warnings);

            if (validation.RequiresClarification || validation.NormalizedIntent.Kind == QueryIntentKinds.Chat)
            {
                pipelineResult.IsChat = true;
                pipelineResult.ChatReply = validation.RequiresClarification
                    ? validation.Clarification
                    : validation.NormalizedIntent.Reply ?? "Сформулируйте аналитический запрос, и я подготовлю intent.";
                return pipelineResult;
            }

            if (validation.ValidatedIntent == null)
                throw new InvalidOperationException("Validated intent is missing.");

            var validatedIntent = validation.ValidatedIntent;
            var builtSql = _sqlBuilder.Build(validatedIntent);
            EnsureDifferentIntentsProduceDifferentSql(previousIntent, validatedIntent, builtSql);

            _logger.LogInformation("Validated intent: {Intent}", JsonSerializer.Serialize(validatedIntent.NormalizedIntent));
            _logger.LogInformation("Generated SQL: {Sql}", builtSql.Sql);

            var explainResult = _explainEngine.Build(validatedIntent, builtSql, userQuery);
            pipelineResult.Intent = validatedIntent.NormalizedIntent;
            pipelineResult.Confidence = validatedIntent.NormalizedIntent.Confidence;
            pipelineResult.Visualization = validatedIntent.Visualization;
            pipelineResult.StructuredExplain = explainResult.Structured;
            pipelineResult.Explain = explainResult.Summary;
            pipelineResult.TechnicalExplain = explainResult.Technical;
            pipelineResult.ReasoningTrail = explainResult.Trail;
            pipelineResult.Sql = FormatSqlForDisplay(builtSql.Sql, builtSql.Parameters);
            pipelineResult.ReasoningTrail.Add(new ReasoningStep(
                "SQL",
                "SQL-запрос",
                "Финальный SQL собран кодом из валидированного intent и semantic layer.",
                pipelineResult.Sql));

            if (validatedIntent.NormalizedIntent.Confidence < 0.5)
            {
                pipelineResult.Warnings.Add(
                    $"Низкая уверенность интерпретации ({validatedIntent.NormalizedIntent.Confidence:P0}). Проверьте, как система поняла запрос.");
            }

            pipelineResult.Result = _queryExecutor.Execute(builtSql, validatedIntent);
            if (pipelineResult.Result.RowCount == 0)
                pipelineResult.Warnings.Add("Запрос выполнен, но данных по выбранным условиям нет.");
        }
        catch (Exception exception)
        {
            _logger.LogError(exception, "NL→SQL pipeline failed.");
            pipelineResult.Error = exception.Message;
        }

        return pipelineResult;
    }

    private static bool RequiresWriteAccess(string userQuery) =>
        !string.IsNullOrWhiteSpace(userQuery) && WriteIntentPattern.IsMatch(userQuery);

    public PipelineResult ReplayFromReport(string intentJson, CancellationToken cancellationToken = default)
    {
        var storedIntent = JsonSerializer.Deserialize<QueryIntent>(intentJson, new JsonSerializerOptions
        {
            PropertyNameCaseInsensitive = true
        }) ?? throw new InvalidOperationException("Bad report intent JSON.");

        var validatedIntent = _intentValidator.ValidateStoredIntent(storedIntent);
        var builtSql = _sqlBuilder.Build(validatedIntent);
        var explainResult = _explainEngine.Build(validatedIntent, builtSql, string.Empty, isReplay: true);

        var pipelineResult = new PipelineResult
        {
            Intent = validatedIntent.NormalizedIntent,
            Confidence = validatedIntent.NormalizedIntent.Confidence,
            Visualization = validatedIntent.Visualization,
            StructuredExplain = explainResult.Structured,
            Explain = explainResult.Summary,
            TechnicalExplain = explainResult.Technical,
            ReasoningTrail = explainResult.Trail,
            Sql = FormatSqlForDisplay(builtSql.Sql, builtSql.Parameters),
            Result = _queryExecutor.Execute(builtSql, validatedIntent)
        };

        pipelineResult.ReasoningTrail.Add(new ReasoningStep(
            "SQL",
            "SQL-запрос",
            "SQL пересобран кодом из сохранённого intent.",
            pipelineResult.Sql));

        return pipelineResult;
    }

    private void EnsureDifferentIntentsProduceDifferentSql(QueryIntent? previousIntent, ValidatedIntent currentIntent, BuiltSql currentSql)
    {
        if (previousIntent == null || !string.Equals(previousIntent.Kind, QueryIntentKinds.Query, StringComparison.OrdinalIgnoreCase))
            return;

        try
        {
            var validatedPreviousIntent = _intentValidator.ValidateStoredIntent(previousIntent);
            var previousSql = _sqlBuilder.Build(validatedPreviousIntent);
            SqlGuard.EnsureDifferentIntentProducesDifferentSql(validatedPreviousIntent, previousSql, currentIntent, currentSql);
        }
        catch (InvalidOperationException exception) when (!exception.Message.StartsWith("Guardrails:", StringComparison.Ordinal))
        {
            _logger.LogDebug(exception, "Skipping previous intent differentiation check because stored previous intent could not be validated.");
        }
    }

    private static string FormatSqlForDisplay(string sql, IReadOnlyDictionary<string, object?> parameters)
    {
        var formattedSql = sql;
        foreach (var parameter in parameters.OrderByDescending(item => item.Key.Length))
        {
            var renderedValue = parameter.Value switch
            {
                null => "NULL",
                string stringValue => $"'{stringValue.Replace("'", "''", StringComparison.Ordinal)}'",
                bool boolValue => boolValue ? "1" : "0",
                _ => parameter.Value.ToString() ?? "NULL"
            };

            formattedSql = formattedSql.Replace(parameter.Key, renderedValue, StringComparison.Ordinal);
        }

        return formattedSql;
    }
}
