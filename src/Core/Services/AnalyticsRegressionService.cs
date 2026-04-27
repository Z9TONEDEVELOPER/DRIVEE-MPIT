using NexusDataSpace.Core.Models;

namespace NexusDataSpace.Core.Services;

public sealed record AnalyticsRegressionRunResult(
    DateTimeOffset CheckedAt,
    bool Passed,
    int PassedCount,
    int FailedCount,
    IReadOnlyList<AnalyticsRegressionCaseResult> Cases);

public sealed record AnalyticsRegressionCaseResult(
    string Id,
    string Query,
    bool Passed,
    string? Metric,
    IReadOnlyList<string> Dimensions,
    string? Period,
    string? Sql,
    IReadOnlyList<string> Errors);

internal sealed record AnalyticsRegressionCase(
    string Id,
    string Query,
    string ExpectedMetric,
    IReadOnlyList<string> ExpectedDimensions,
    IReadOnlyList<string> ExpectedPeriods,
    IReadOnlyList<string> ExpectedSqlContains,
    bool ExpectedComparison = false,
    int? ExpectedLimit = null);

public sealed class AnalyticsRegressionService
{
    private readonly LlmService _llmService;
    private readonly SemanticLayer _semanticLayer;
    private readonly IntentValidator _intentValidator;
    private readonly SqlBuilder _sqlBuilder;
    private readonly TenantContext _tenantContext;

    private static readonly AnalyticsRegressionCase[] Cases =
    {
        new(
            "revenue-current-vs-previous-month",
            "Сравни выручку за этот и прошлый месяц",
            "revenue_sum",
            Array.Empty<string>(),
            new[] { "current_month", "previous_month" },
            new[] { "price_order_local", "status_order", "done" },
            ExpectedComparison: true),
        new(
            "cancel-rate-last-30-days-by-day",
            "Доля отмен по дням за последние 30 дней",
            "cancellation_rate",
            new[] { "day" },
            new[] { "last_n_days:30" },
            new[] { "status_order", "cancel", "delete", "date(" }),
        new(
            "top-cities-cancelled-orders",
            "Топ 5 городов по отменам за последние 30 дней",
            "orders_count",
            new[] { "city" },
            new[] { "last_n_days:30" },
            new[] { "city_id", "status_order", "cancel", "delete" },
            ExpectedLimit: 5),
        new(
            "orders-last-3-days-by-day",
            "Количество заказов по дням за последние 3 дня",
            "orders_count",
            new[] { "day" },
            new[] { "last_n_days:3" },
            new[] { "COUNT", "date(" })
    };

    public AnalyticsRegressionService(
        LlmService llmService,
        SemanticLayer semanticLayer,
        IntentValidator intentValidator,
        SqlBuilder sqlBuilder,
        TenantContext tenantContext)
    {
        _llmService = llmService;
        _semanticLayer = semanticLayer;
        _intentValidator = intentValidator;
        _sqlBuilder = sqlBuilder;
        _tenantContext = tenantContext;
    }

    public async Task<AnalyticsRegressionRunResult> RunAsync(int? companyId = null, CancellationToken cancellationToken = default)
    {
        using var tenantScope = companyId.HasValue ? _tenantContext.Use(companyId.Value) : null;
        var results = new List<AnalyticsRegressionCaseResult>();

        foreach (var testCase in Cases)
            results.Add(await RunCaseAsync(testCase, cancellationToken));

        var passed = results.Count(result => result.Passed);
        var failed = results.Count - passed;
        return new AnalyticsRegressionRunResult(
            DateTimeOffset.UtcNow,
            failed == 0,
            passed,
            failed,
            results);
    }

    private async Task<AnalyticsRegressionCaseResult> RunCaseAsync(
        AnalyticsRegressionCase testCase,
        CancellationToken cancellationToken)
    {
        var errors = new List<string>();
        QueryIntent? normalizedIntent = null;
        string? sql = null;

        try
        {
            var rawIntent = await _llmService.InterpretAsync(
                testCase.Query,
                PromptTemplates.SystemPrompt(_semanticLayer),
                history: null,
                cancellationToken);

            var validation = _intentValidator.ValidateParsedIntent(rawIntent, testCase.Query);
            normalizedIntent = validation.NormalizedIntent;
            if (validation.ValidatedIntent == null)
            {
                errors.Add(validation.Clarification ?? "Intent did not validate.");
            }
            else
            {
                var builtSql = _sqlBuilder.Build(validation.ValidatedIntent);
                sql = builtSql.Sql;
                ValidateCase(testCase, validation.ValidatedIntent, builtSql.Sql, errors);
            }
        }
        catch (Exception exception) when (exception is InvalidOperationException or HttpRequestException or TaskCanceledException)
        {
            errors.Add(exception.Message);
        }

        return new AnalyticsRegressionCaseResult(
            testCase.Id,
            testCase.Query,
            errors.Count == 0,
            normalizedIntent?.Metric,
            normalizedIntent?.Dimensions.ToArray() ?? Array.Empty<string>(),
            BuildPeriodLabel(normalizedIntent),
            sql,
            errors);
    }

    private static void ValidateCase(
        AnalyticsRegressionCase testCase,
        ValidatedIntent intent,
        string sql,
        List<string> errors)
    {
        if (!string.Equals(intent.Metric.Key, testCase.ExpectedMetric, StringComparison.OrdinalIgnoreCase))
            errors.Add($"Metric: expected `{testCase.ExpectedMetric}`, got `{intent.Metric.Key}`.");

        foreach (var expectedDimension in testCase.ExpectedDimensions)
        {
            if (intent.Dimensions.All(dimension => !dimension.Definition.Key.Equals(expectedDimension, StringComparison.OrdinalIgnoreCase)))
                errors.Add($"Missing dimension `{expectedDimension}`.");
        }

        if (testCase.ExpectedComparison && intent.ComparisonRanges.Count < 2)
            errors.Add("Expected period comparison with two ranges.");

        foreach (var expectedPeriod in testCase.ExpectedPeriods)
        {
            if (!MatchesPeriod(intent, expectedPeriod))
                errors.Add($"Missing period `{expectedPeriod}`.");
        }

        if (testCase.ExpectedLimit.HasValue && intent.Limit != testCase.ExpectedLimit.Value)
            errors.Add($"Limit: expected `{testCase.ExpectedLimit.Value}`, got `{intent.Limit}`.");

        foreach (var sqlPart in testCase.ExpectedSqlContains)
        {
            if (!sql.Contains(sqlPart, StringComparison.OrdinalIgnoreCase))
                errors.Add($"SQL does not contain `{sqlPart}`.");
        }
    }

    private static bool MatchesPeriod(ValidatedIntent intent, string expectedPeriod)
    {
        if (expectedPeriod.Contains(':', StringComparison.Ordinal))
        {
            var parts = expectedPeriod.Split(':', 2);
            return intent.NormalizedIntent.DateRange?.Type?.Equals(parts[0], StringComparison.OrdinalIgnoreCase) == true &&
                   intent.NormalizedIntent.DateRange.Value?.ToString() == parts[1];
        }

        if (intent.NormalizedIntent.Periods?.Any(period => period.Equals(expectedPeriod, StringComparison.OrdinalIgnoreCase)) == true)
            return true;

        return intent.NormalizedIntent.DateRange?.Type?.Equals(expectedPeriod, StringComparison.OrdinalIgnoreCase) == true;
    }

    private static string? BuildPeriodLabel(QueryIntent? intent)
    {
        if (intent == null)
            return null;

        if (intent.Periods?.Count > 0)
            return string.Join(" vs ", intent.Periods);

        if (!string.IsNullOrWhiteSpace(intent.DateRange?.Type))
            return intent.DateRange.Value.HasValue
                ? $"{intent.DateRange.Type}:{intent.DateRange.Value}"
                : intent.DateRange.Type;

        return intent.Period;
    }
}
