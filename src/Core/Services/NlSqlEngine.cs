using System.Diagnostics;
using System.Globalization;
using System.Text.Json;
using System.Text.RegularExpressions;
using NexusDataSpace.Core.Models;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace NexusDataSpace.Core.Services;

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
    private readonly DataSourceService _dataSources;
    private readonly TenantContext _tenantContext;
    private readonly QueryLoadControl _loadControl;
    private readonly OperationalMetricsService _metrics;
    private readonly IFewShotProvider _fewShotProvider;
    private readonly IFewShotRanker _fewShotRanker;
    private readonly IFewShotFeedbackStore _fewShotFeedbackStore;
    private readonly int _fewShotMaxExamples;
    private readonly int _fewShotCandidateLimit;
    private readonly ILogger<NlSqlEngine> _logger;

    public NlSqlEngine(
        LlmService llmService,
        SemanticLayer semanticLayer,
        IntentValidator intentValidator,
        SqlBuilder sqlBuilder,
        ExplainEngine explainEngine,
        QueryExecutor queryExecutor,
        DataSourceService dataSources,
        TenantContext tenantContext,
        QueryLoadControl loadControl,
        OperationalMetricsService metrics,
        IFewShotProvider fewShotProvider,
        IFewShotRanker fewShotRanker,
        IFewShotFeedbackStore fewShotFeedbackStore,
        IConfiguration configuration,
        ILogger<NlSqlEngine> logger)
    {
        _llmService = llmService;
        _semanticLayer = semanticLayer;
        _intentValidator = intentValidator;
        _sqlBuilder = sqlBuilder;
        _explainEngine = explainEngine;
        _queryExecutor = queryExecutor;
        _dataSources = dataSources;
        _tenantContext = tenantContext;
        _loadControl = loadControl;
        _metrics = metrics;
        _fewShotProvider = fewShotProvider;
        _fewShotRanker = fewShotRanker;
        _fewShotFeedbackStore = fewShotFeedbackStore;
        _fewShotMaxExamples = ReadBoundedInt(configuration, "Llm:FewShotMaxExamples", 6, 0, 32);
        _fewShotCandidateLimit = Math.Max(
            _fewShotMaxExamples,
            ReadBoundedInt(configuration, "Llm:FewShotCandidateLimit", Math.Max(16, _fewShotMaxExamples), 0, 128));
        _logger = logger;
    }

    private static int ReadBoundedInt(
        IConfiguration configuration,
        string key,
        int fallback,
        int min,
        int max)
    {
        if (int.TryParse(configuration[key], NumberStyles.Integer, CultureInfo.InvariantCulture, out var value))
            return Math.Clamp(value, min, max);
        return fallback;
    }

    public async Task<PipelineResult> RunAsync(
        string userQuery,
        IReadOnlyList<ChatTurn>? history = null,
        QueryIntent? previousIntent = null,
        CancellationToken cancellationToken = default,
        int? companyId = null,
        string? userKey = null)
    {
        using var tenantScope = companyId.HasValue ? _tenantContext.Use(companyId.Value) : null;
        using var activeRequestScope = _metrics.EnterRequest();
        var pipelineResult = new PipelineResult { UserQuery = userQuery };
        var totalStopwatch = Stopwatch.StartNew();
        var effectiveCompanyId = companyId ?? _tenantContext.CompanyId;

        try
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!_loadControl.TryAcquire(effectiveCompanyId, userKey, out var retryAfter))
            {
                _metrics.RecordRateLimited(effectiveCompanyId, userKey);
                pipelineResult.Error = BuildRateLimitMessage(retryAfter);
                return pipelineResult;
            }

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

            _dataSources.EnsureActiveSourceReadyForAnalytics(effectiveCompanyId);
            cancellationToken.ThrowIfCancellationRequested();

            IReadOnlyList<FewShotExample> fewShotExamples = Array.Empty<FewShotExample>();
            if (_fewShotMaxExamples > 0)
            {
                var fewShotCandidates = _fewShotProvider.SelectFor(userQuery, _fewShotCandidateLimit);
                fewShotExamples = await _fewShotRanker.RankAsync(
                    userQuery,
                    fewShotCandidates,
                    _fewShotMaxExamples,
                    cancellationToken);
                _logger.LogInformation(
                    "Few-shot examples selected. selected={Selected}, candidates={Candidates}, ranker={Ranker}",
                    fewShotExamples.Count,
                    fewShotCandidates.Count,
                    _fewShotRanker.GetType().Name);
            }
            var systemPrompt = PromptTemplates.SystemPrompt(_semanticLayer, fewShotExamples);
            var llmStopwatch = Stopwatch.StartNew();
            var rawIntent = await _llmService.InterpretAsync(
                userQuery,
                systemPrompt,
                history,
                cancellationToken);
            llmStopwatch.Stop();
            _logger.LogInformation("Pipeline timing: stage=llm elapsed_ms={ElapsedMs}", llmStopwatch.ElapsedMilliseconds);

            var validation = _intentValidator.ValidateParsedIntent(rawIntent, userQuery, previousIntent);
            cancellationToken.ThrowIfCancellationRequested();

            if (ShouldAttemptRepair(rawIntent, validation))
            {
                var repairStopwatch = Stopwatch.StartNew();
                var repairedIntent = await _llmService.RepairIntentAsync(
                    userQuery,
                    systemPrompt,
                    rawIntent,
                    validation.Clarification ?? "Intent failed validation.",
                    history,
                    cancellationToken);
                repairStopwatch.Stop();
                _logger.LogInformation(
                    "Pipeline timing: stage=llm_repair elapsed_ms={ElapsedMs} attempted={Attempted}",
                    repairStopwatch.ElapsedMilliseconds,
                    repairedIntent != null);

                if (repairedIntent != null)
                {
                    var repairedValidation = _intentValidator.ValidateParsedIntent(repairedIntent, userQuery, previousIntent);
                    cancellationToken.ThrowIfCancellationRequested();
                    if (repairedValidation.ValidatedIntent != null)
                    {
                        _logger.LogInformation("LLM self-repair fixed intent on retry.");
                        rawIntent = repairedIntent;
                        validation = repairedValidation;
                    }
                    else
                    {
                        _logger.LogInformation(
                            "LLM self-repair retry did not fix intent. clarification={Clarification}",
                            repairedValidation.Clarification);
                    }
                }
            }

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
            var sqlBuildStopwatch = Stopwatch.StartNew();
            var builtSql = _sqlBuilder.Build(validatedIntent);
            cancellationToken.ThrowIfCancellationRequested();
            EnsureDifferentIntentsProduceDifferentSql(previousIntent, validatedIntent, builtSql);
            sqlBuildStopwatch.Stop();
            _logger.LogInformation("Pipeline timing: stage=sql_build elapsed_ms={ElapsedMs}", sqlBuildStopwatch.ElapsedMilliseconds);

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

            var sqlExecuteStopwatch = Stopwatch.StartNew();
            try
            {
                pipelineResult.Result = _queryExecutor.Execute(builtSql, validatedIntent);
            }
            finally
            {
                sqlExecuteStopwatch.Stop();
                _logger.LogInformation(
                    "Pipeline timing: stage=sql_execute elapsed_ms={ElapsedMs} rows={Rows}",
                    sqlExecuteStopwatch.ElapsedMilliseconds,
                    pipelineResult.Result?.RowCount);
            }
            if (pipelineResult.Result.RowCount == 0)
                pipelineResult.Warnings.Add("Запрос выполнен, но данных по выбранным условиям нет.");
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("NL→SQL pipeline canceled.");
            throw;
        }
        catch (Exception exception)
        {
            _logger.LogError(exception, "NL→SQL pipeline failed.");
            pipelineResult.Error = exception.Message;
        }
        finally
        {
            totalStopwatch.Stop();
            _logger.LogInformation(
                "Pipeline timing: stage=total elapsed_ms={ElapsedMs} has_error={HasError}",
                totalStopwatch.ElapsedMilliseconds,
                !string.IsNullOrWhiteSpace(pipelineResult.Error));
            _metrics.RecordQuery(
                effectiveCompanyId,
                userKey,
                string.IsNullOrWhiteSpace(pipelineResult.Error),
                totalStopwatch.ElapsedMilliseconds);
        }

        return pipelineResult;
    }

    private static bool ShouldAttemptRepair(QueryIntent rawIntent, ValidationResult validation)
    {
        if (validation.ValidatedIntent != null)
            return false;

        if (string.IsNullOrWhiteSpace(validation.Clarification))
            return false;

        var rawKind = rawIntent.Kind?.Trim().ToLowerInvariant();
        if (string.IsNullOrEmpty(rawKind))
            rawKind = QueryIntentKinds.Query;

        if (!string.Equals(rawKind, QueryIntentKinds.Query, StringComparison.Ordinal))
            return false;

        return !string.IsNullOrWhiteSpace(rawIntent.Metric)
               || rawIntent.Dimensions.Count > 0
               || rawIntent.Filters.Count > 0
               || rawIntent.DateRange != null
               || rawIntent.Limit.HasValue
               || rawIntent.Sort.Count > 0;
    }

    private static bool RequiresWriteAccess(string userQuery) =>
        !string.IsNullOrWhiteSpace(userQuery) && WriteIntentPattern.IsMatch(userQuery);

    private void CaptureCorrectedIntentFeedback(int companyId, string userQuery, QueryIntent normalizedIntent)
    {
        if (string.IsNullOrWhiteSpace(userQuery))
            return;

        try
        {
            _fewShotFeedbackStore.Save(companyId, userQuery, normalizedIntent);
            _logger.LogInformation(
                "Captured corrected-intent feedback for few-shot bank. company_id={CompanyId}, metric={Metric}",
                companyId,
                normalizedIntent.Metric);
        }
        catch (Exception exception)
        {
            _logger.LogWarning(exception, "Failed to capture corrected-intent feedback.");
        }
    }

    private static string BuildRateLimitMessage(TimeSpan retryAfter) =>
        $"Too many analytics requests. Try again in {Math.Max(1, (int)Math.Ceiling(retryAfter.TotalSeconds))} sec.";

    public PipelineResult ReplayFromReport(
        string intentJson,
        CancellationToken cancellationToken = default,
        int? companyId = null,
        string? userKey = null)
    {
        using var tenantScope = companyId.HasValue ? _tenantContext.Use(companyId.Value) : null;
        using var activeRequestScope = _metrics.EnterRequest();
        var effectiveCompanyId = companyId ?? _tenantContext.CompanyId;
        var stopwatch = Stopwatch.StartNew();
        if (!_loadControl.TryAcquire(effectiveCompanyId, userKey, out var retryAfter))
        {
            stopwatch.Stop();
            _metrics.RecordRateLimited(effectiveCompanyId, userKey);
            _metrics.RecordQuery(effectiveCompanyId, userKey, false, stopwatch.ElapsedMilliseconds);
            return new PipelineResult
            {
                Error = BuildRateLimitMessage(retryAfter)
            };
        }

        _dataSources.EnsureActiveSourceReadyForAnalytics(effectiveCompanyId);

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

        stopwatch.Stop();
        _metrics.RecordQuery(effectiveCompanyId, userKey, true, stopwatch.ElapsedMilliseconds);
        return pipelineResult;
    }

    public PipelineResult RunCorrectedIntent(
        QueryIntent correctedIntent,
        string userQuery,
        CancellationToken cancellationToken = default,
        int? companyId = null,
        string? userKey = null)
    {
        using var tenantScope = companyId.HasValue ? _tenantContext.Use(companyId.Value) : null;
        using var activeRequestScope = _metrics.EnterRequest();
        var pipelineResult = new PipelineResult { UserQuery = userQuery };
        var effectiveCompanyId = companyId ?? _tenantContext.CompanyId;
        var stopwatch = Stopwatch.StartNew();

        if (!_loadControl.TryAcquire(effectiveCompanyId, userKey, out var retryAfter))
        {
            stopwatch.Stop();
            _metrics.RecordRateLimited(effectiveCompanyId, userKey);
            pipelineResult.Error = BuildRateLimitMessage(retryAfter);
            _metrics.RecordQuery(effectiveCompanyId, userKey, false, stopwatch.ElapsedMilliseconds);
            return pipelineResult;
        }

        try
        {
            _dataSources.EnsureActiveSourceReadyForAnalytics(effectiveCompanyId);
            correctedIntent.Kind = QueryIntentKinds.Query;
            correctedIntent.Confidence = Math.Max(correctedIntent.Confidence, 0.95);
            correctedIntent.Explanation = "Intent corrected by user in UI.";

            var validatedIntent = _intentValidator.ValidateStoredIntent(correctedIntent);
            var builtSql = _sqlBuilder.Build(validatedIntent);
            var explainResult = _explainEngine.Build(validatedIntent, builtSql, userQuery);

            pipelineResult.Intent = validatedIntent.NormalizedIntent;
            pipelineResult.Confidence = validatedIntent.NormalizedIntent.Confidence;
            pipelineResult.Visualization = validatedIntent.Visualization;
            pipelineResult.StructuredExplain = explainResult.Structured;
            pipelineResult.Explain = explainResult.Summary;
            pipelineResult.TechnicalExplain = explainResult.Technical;
            pipelineResult.ReasoningTrail = explainResult.Trail;
            pipelineResult.Sql = FormatSqlForDisplay(builtSql.Sql, builtSql.Parameters);
            pipelineResult.Result = _queryExecutor.Execute(builtSql, validatedIntent);
            pipelineResult.ReasoningTrail.Add(new ReasoningStep(
                "SQL",
                "SQL-запрос",
                "SQL пересобран после ручного исправления интерпретации.",
                pipelineResult.Sql));

            CaptureCorrectedIntentFeedback(effectiveCompanyId, userQuery, validatedIntent.NormalizedIntent);
        }
        catch (Exception exception)
        {
            _logger.LogError(exception, "Corrected intent execution failed.");
            pipelineResult.Error = exception.Message;
        }
        finally
        {
            stopwatch.Stop();
            _metrics.RecordQuery(
                effectiveCompanyId,
                userKey,
                string.IsNullOrWhiteSpace(pipelineResult.Error),
                stopwatch.ElapsedMilliseconds);
        }

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
