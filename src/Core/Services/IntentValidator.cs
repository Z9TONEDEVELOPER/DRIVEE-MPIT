using System.Globalization;
using System.Text.Json;
using System.Text.RegularExpressions;
using NexusDataSpace.Core.Models;
using Microsoft.Extensions.Configuration;

namespace NexusDataSpace.Core.Services;

public sealed record ValidationResult(
    QueryIntent NormalizedIntent,
    ValidatedIntent? ValidatedIntent,
    string? Clarification,
    List<string> Warnings)
{
    public bool RequiresClarification => !string.IsNullOrWhiteSpace(Clarification);
}

public sealed record ValidatedDimension(
    DimensionDefinition Definition,
    string SqlExpression,
    string Alias);

public sealed record ValidatedFilter(
    FilterDefinition Definition,
    string Operator,
    IReadOnlyList<object?> Values,
    string Label);

public sealed record ValidatedSort(
    string FieldKey,
    string Alias,
    string Direction,
    string Label);

public sealed record ValidatedIntent(
    string UserQuery,
    QueryIntent NormalizedIntent,
    MetricDefinition Metric,
    string Aggregation,
    SourceDefinition Source,
    IReadOnlyList<ValidatedDimension> Dimensions,
    IReadOnlyList<ValidatedFilter> Filters,
    ResolvedDateRange? DateRange,
    IReadOnlyList<ResolvedDateRange> ComparisonRanges,
    IReadOnlyList<ValidatedSort> Sort,
    int Limit,
    string Visualization,
    string DiscriminatorKey);

public class IntentValidator
{
    private readonly SemanticLayer _semanticLayer;
    private readonly DateResolver _dateResolver;
    private readonly int _maxRows;
    private readonly int _defaultLimit;

    public IntentValidator(SemanticLayer semanticLayer, DateResolver dateResolver, IConfiguration configuration)
    {
        _semanticLayer = semanticLayer;
        _dateResolver = dateResolver;
        _maxRows = int.TryParse(configuration["Data:MaxRows"], out var maxRows) ? maxRows : 10000;
        _defaultLimit = int.TryParse(configuration["Data:DefaultLimit"], out var defaultLimit) ? defaultLimit : _maxRows;
    }

    public ValidationResult ValidateParsedIntent(QueryIntent rawIntent, string userQuery, QueryIntent? previousIntent = null)
    {
        var warnings = new List<string>();
        var canonicalIntent = CreateCanonicalIntent(rawIntent, userQuery, previousIntent, warnings, fromStoredIntent: false);

        if (canonicalIntent.Kind == QueryIntentKinds.Chat)
        {
            canonicalIntent.Reply ??= "Я готов помочь с аналитическим запросом.";
            return new ValidationResult(canonicalIntent, null, canonicalIntent.Reply, warnings);
        }

        if (canonicalIntent.Kind == QueryIntentKinds.Clarify || !string.IsNullOrWhiteSpace(canonicalIntent.Clarification))
        {
            var clarification = canonicalIntent.Clarification ?? canonicalIntent.Reply ?? "Уточните запрос.";
            canonicalIntent.Kind = QueryIntentKinds.Clarify;
            canonicalIntent.Reply = clarification;
            canonicalIntent.Clarification = clarification;
            canonicalIntent.Confidence = Math.Min(canonicalIntent.Confidence, 0.35);
            return new ValidationResult(canonicalIntent, null, clarification, warnings);
        }

        if (!TryBuildValidatedIntent(canonicalIntent, userQuery, warnings, allowClarification: true, out var validatedIntent, out var clarificationMessage))
        {
            canonicalIntent.Kind = QueryIntentKinds.Clarify;
            canonicalIntent.Reply = clarificationMessage;
            canonicalIntent.Clarification = clarificationMessage;
            canonicalIntent.Confidence = Math.Min(canonicalIntent.Confidence, 0.35);
            return new ValidationResult(canonicalIntent, null, clarificationMessage, warnings);
        }

        return new ValidationResult(validatedIntent!.NormalizedIntent, validatedIntent, null, warnings);
    }

    public ValidatedIntent ValidateStoredIntent(QueryIntent storedIntent)
    {
        var warnings = new List<string>();
        var canonicalIntent = CreateCanonicalIntent(storedIntent, string.Empty, null, warnings, fromStoredIntent: true);

        if (!TryBuildValidatedIntent(canonicalIntent, string.Empty, warnings, allowClarification: false, out var validatedIntent, out var clarificationMessage) ||
            validatedIntent == null)
        {
            throw new InvalidOperationException(clarificationMessage ?? "Stored intent is invalid.");
        }

        return validatedIntent;
    }

    private QueryIntent CreateCanonicalIntent(
        QueryIntent rawIntent,
        string userQuery,
        QueryIntent? previousIntent,
        List<string> warnings,
        bool fromStoredIntent)
    {
        var canonicalIntent = new QueryIntent
        {
            Kind = NormalizeKind(rawIntent.Kind),
            Reply = rawIntent.Reply,
            Clarification = rawIntent.Clarification,
            Intent = NormalizeIntentType(rawIntent.Intent),
            Metric = TrimOrNull(rawIntent.Metric),
            Aggregation = TrimOrNull(rawIntent.Aggregation)?.ToLowerInvariant(),
            Dimensions = rawIntent.Dimensions
                .Where(dimension => !string.IsNullOrWhiteSpace(dimension))
                .Select(dimension => dimension.Trim())
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToList(),
            Filters = rawIntent.Filters
                .Where(filter => !string.IsNullOrWhiteSpace(filter.Field))
                .Select(CloneFilter)
                .ToList(),
            DateRange = CloneDateRange(rawIntent.DateRange),
            Sort = rawIntent.Sort
                .Where(sort => !string.IsNullOrWhiteSpace(sort.Field))
                .Select(CloneSort)
                .ToList(),
            Limit = rawIntent.Limit,
            Visualization = TrimOrNull(rawIntent.Visualization ?? rawIntent.VisualizationHint)?.ToLowerInvariant(),
            Source = TrimOrNull(rawIntent.Source),
            Comparison = CloneComparison(rawIntent.Comparison),
            Confidence = ClampConfidence(rawIntent.Confidence),
            Explanation = rawIntent.Explanation,
            GroupBy = TrimOrNull(rawIntent.GroupBy),
            Period = TrimOrNull(rawIntent.Period),
            Periods = rawIntent.Periods?.Where(period => !string.IsNullOrWhiteSpace(period)).Select(period => period.Trim()).ToList(),
            VisualizationHint = TrimOrNull(rawIntent.VisualizationHint)?.ToLowerInvariant(),
            ExtraFields = rawIntent.ExtraFields == null ? null : new Dictionary<string, JsonElement>(rawIntent.ExtraFields)
        };

        if (!string.IsNullOrWhiteSpace(canonicalIntent.GroupBy) &&
            canonicalIntent.Dimensions.All(dimension => !dimension.Equals(canonicalIntent.GroupBy, StringComparison.OrdinalIgnoreCase)))
        {
            canonicalIntent.Dimensions.Add(canonicalIntent.GroupBy);
        }

        if (canonicalIntent.Kind == QueryIntentKinds.Query)
        {
            ApplySemanticPresets(canonicalIntent, userQuery);

            canonicalIntent.Metric ??= _semanticLayer.MatchMetricInText(userQuery)?.Key;
        }

        if (!fromStoredIntent && previousIntent != null && LooksLikeRefinement(userQuery, canonicalIntent))
            ApplyPreviousContext(canonicalIntent, previousIntent);

        if (!fromStoredIntent && canonicalIntent.Kind == QueryIntentKinds.Query)
            ApplyQueryTextHints(canonicalIntent, userQuery);

        if (canonicalIntent.Kind == QueryIntentKinds.Query)
        {
            canonicalIntent.Metric ??= _semanticLayer.MatchMetricInText(userQuery)?.Key;
            if (canonicalIntent.Dimensions.Count == 0)
            {
                var inferredDimension = _semanticLayer.MatchDimensionInText(userQuery);
                if (inferredDimension != null)
                    canonicalIntent.Dimensions.Add(inferredDimension.Key);
            }
        }

        if (canonicalIntent.Confidence <= 0)
            canonicalIntent.Confidence = fromStoredIntent ? 0.95 : 0.5;

        if (canonicalIntent.Sort.Count == 0 && canonicalIntent.Limit is > 0 && canonicalIntent.Dimensions.Count > 0)
        {
            canonicalIntent.Sort.Add(new QuerySort
            {
                Field = canonicalIntent.Metric ?? "metric",
                Direction = "desc"
            });
        }

        return canonicalIntent;
    }

    private bool TryBuildValidatedIntent(
        QueryIntent canonicalIntent,
        string userQuery,
        List<string> warnings,
        bool allowClarification,
        out ValidatedIntent? validatedIntent,
        out string? clarificationMessage)
    {
        validatedIntent = null;
        clarificationMessage = null;

        if (HasUnexpectedFields(canonicalIntent, out var unexpectedFields))
        {
            clarificationMessage = $"Система получила неподдерживаемую структуру intent: {string.Join(", ", unexpectedFields)}.";
            return false;
        }

        var metricDefinition = ResolveMetric(canonicalIntent, userQuery);
        if (metricDefinition == null)
        {
            clarificationMessage = "Уточните, какую метрику показать: заказы, выручку, средний чек, длительность, дистанцию или долю отмен.";
            return false;
        }

        if (string.Equals(metricDefinition.Key, "cancellation_rate", StringComparison.OrdinalIgnoreCase))
            canonicalIntent.Filters.RemoveAll(filter => string.Equals(filter.Field, "status_order", StringComparison.OrdinalIgnoreCase));

        var sourceDefinition = _semanticLayer.ResolveSource(canonicalIntent.Source ?? metricDefinition.Source);
        if (sourceDefinition == null)
        {
            clarificationMessage = "Не удалось определить источник данных.";
            return false;
        }

        var aggregation = string.IsNullOrWhiteSpace(canonicalIntent.Aggregation)
            ? metricDefinition.Aggregation
            : canonicalIntent.Aggregation.Trim().ToLowerInvariant();

        if (!aggregation.Equals(metricDefinition.Aggregation, StringComparison.OrdinalIgnoreCase))
        {
            warnings.Add($"Агрегация `{aggregation}` не поддерживается для метрики `{metricDefinition.Key}`. Использую `{metricDefinition.Aggregation}`.");
            aggregation = metricDefinition.Aggregation;
        }

        if (allowClarification &&
            _dateResolver.TryBuildAmbiguousPeriodClarification(userQuery, out clarificationMessage))
        {
            return false;
        }

        var dateRangeSpec = canonicalIntent.DateRange ?? _dateResolver.NormalizeLegacy(canonicalIntent.Period, metricDefinition.DateColumn);
        if (dateRangeSpec == null && !string.IsNullOrWhiteSpace(userQuery) &&
            _dateResolver.TryExtractDateRange(userQuery, metricDefinition.DateColumn, out var inferredDateRange))
        {
            dateRangeSpec = inferredDateRange;
        }

        if (dateRangeSpec != null && string.IsNullOrWhiteSpace(dateRangeSpec.DateColumn))
            dateRangeSpec.DateColumn = metricDefinition.DateColumn;

        var resolvedDateRange = canonicalIntent.Intent == QueryIntentKinds.ComparePeriods
            ? null
            : _dateResolver.Resolve(dateRangeSpec, metricDefinition.DateColumn, sourceDefinition.Table);

        var comparisonRanges = canonicalIntent.Intent == QueryIntentKinds.ComparePeriods
            ? _dateResolver.ResolveComparisonPeriods(canonicalIntent, metricDefinition.DateColumn, sourceDefinition.Table)
            : Array.Empty<ResolvedDateRange>();

        if (canonicalIntent.Intent == QueryIntentKinds.ComparePeriods && comparisonRanges.Count < 2)
        {
            clarificationMessage = "Уточните, какие два периода нужно сравнить.";
            return false;
        }

        var validatedDimensions = new List<ValidatedDimension>();
        foreach (var dimensionKey in BuildDimensionKeys(canonicalIntent))
        {
            var dimensionDefinition = _semanticLayer.ResolveDimension(dimensionKey);
            if (dimensionDefinition == null)
            {
                clarificationMessage = $"Неизвестная группировка: {dimensionKey}.";
                return false;
            }

            if (!metricDefinition.AllowedDimensions.Contains(dimensionDefinition.Key))
            {
                clarificationMessage = $"Метрику «{metricDefinition.DisplayLabel}» нельзя группировать по «{dimensionDefinition.DisplayLabel}».";
                return false;
            }

            if (validatedDimensions.Any(existing => existing.Definition.Key.Equals(dimensionDefinition.Key, StringComparison.OrdinalIgnoreCase)))
                continue;

            var sqlExpression = _semanticLayer.RenderDimensionSql(dimensionDefinition, resolvedDateRange?.DateColumn ?? metricDefinition.DateColumn);
            validatedDimensions.Add(new ValidatedDimension(dimensionDefinition, sqlExpression, dimensionDefinition.Key));
        }

        if (validatedDimensions.Count > 2)
        {
            clarificationMessage = "Сейчас поддерживается не более двух группировок одновременно. Уточните запрос.";
            return false;
        }

        var validatedFilters = new List<ValidatedFilter>();
        foreach (var filter in MergeFilters(metricDefinition, canonicalIntent.Filters))
        {
            if (!TryBuildValidatedFilter(filter, metricDefinition, out var validatedFilter, out clarificationMessage))
                return false;

            validatedFilters.Add(validatedFilter!);
        }

        var limit = Math.Clamp(canonicalIntent.Limit ?? _defaultLimit, 1, _maxRows);
        var validatedSorts = BuildValidatedSorts(canonicalIntent, metricDefinition, validatedDimensions, comparisonRanges.Count > 0, limit, out clarificationMessage);
        if (validatedSorts == null)
            return false;

        var visualization = NormalizeVisualization(canonicalIntent, validatedDimensions, comparisonRanges.Count > 0, userQuery);
        var confidence = ClampConfidence(canonicalIntent.Confidence);

        if (allowClarification &&
            NeedsPeriodClarification(userQuery, resolvedDateRange, comparisonRanges, validatedDimensions, validatedFilters))
        {
            clarificationMessage = $"Уточните, за какой период показать {metricDefinition.DisplayLabel.ToLowerInvariant()}?";
            return false;
        }

        var normalizedIntent = new QueryIntent
        {
            Kind = QueryIntentKinds.Query,
            Intent = comparisonRanges.Count > 0 ? QueryIntentKinds.ComparePeriods : QueryIntentKinds.MetricQuery,
            Metric = metricDefinition.Key,
            Aggregation = aggregation,
            Dimensions = validatedDimensions.Select(dimension => dimension.Definition.Key).ToList(),
            Filters = validatedFilters.Select(ToIntentFilter).ToList(),
            DateRange = comparisonRanges.Count > 0 ? null : CanonicalizeDateRange(dateRangeSpec, resolvedDateRange),
            Sort = validatedSorts.Select(sort => new QuerySort { Field = sort.FieldKey, Direction = sort.Direction }).ToList(),
            Limit = limit,
            Visualization = visualization,
            VisualizationHint = visualization,
            Source = sourceDefinition.Key,
            Comparison = comparisonRanges.Count > 0 ? BuildComparison(canonicalIntent, comparisonRanges, metricDefinition.DateColumn) : CloneComparison(canonicalIntent.Comparison),
            Confidence = confidence,
            Explanation = canonicalIntent.Explanation,
            Reply = canonicalIntent.Reply,
            Clarification = canonicalIntent.Clarification,
            Period = comparisonRanges.Count > 0 ? null : BuildLegacyPeriod(normalizedIntentDateRange: CanonicalizeDateRange(dateRangeSpec, resolvedDateRange)),
            Periods = comparisonRanges.Count > 0 ? BuildLegacyPeriods(canonicalIntent, comparisonRanges, metricDefinition.DateColumn) : null,
            GroupBy = validatedDimensions.FirstOrDefault()?.Definition.Key
        };

        normalizedIntent.DateRange = comparisonRanges.Count > 0 ? null : CanonicalizeDateRange(dateRangeSpec, resolvedDateRange);
        normalizedIntent.Period = comparisonRanges.Count > 0 ? null : BuildLegacyPeriod(normalizedIntent.DateRange);

        var discriminatorKey = BuildDiscriminatorKey(normalizedIntent, comparisonRanges);
        validatedIntent = new ValidatedIntent(
            userQuery,
            normalizedIntent,
            metricDefinition,
            aggregation,
            sourceDefinition,
            validatedDimensions,
            validatedFilters,
            resolvedDateRange,
            comparisonRanges,
            validatedSorts,
            limit,
            visualization,
            discriminatorKey);

        return true;
    }

    private MetricDefinition? ResolveMetric(QueryIntent canonicalIntent, string userQuery)
    {
        var metricDefinition = _semanticLayer.ResolveMetric(canonicalIntent.Metric);
        if (metricDefinition != null)
            return metricDefinition;

        return string.IsNullOrWhiteSpace(userQuery)
            ? null
            : _semanticLayer.MatchMetricInText(userQuery);
    }

    private static string NormalizeKind(string? kind) => kind?.Trim().ToLowerInvariant() switch
    {
        QueryIntentKinds.Chat => QueryIntentKinds.Chat,
        QueryIntentKinds.Clarify => QueryIntentKinds.Clarify,
        _ => QueryIntentKinds.Query
    };

    private static string NormalizeIntentType(string? intent) => intent?.Trim().ToLowerInvariant() switch
    {
        "aggregate" => QueryIntentKinds.MetricQuery,
        QueryIntentKinds.ComparePeriods => QueryIntentKinds.ComparePeriods,
        _ => QueryIntentKinds.MetricQuery
    };

    private static string? TrimOrNull(string? value) =>
        string.IsNullOrWhiteSpace(value) ? null : value.Trim();

    private static double ClampConfidence(double confidence) =>
        Math.Clamp(confidence <= 0 ? 0.5 : confidence, 0.05, 0.99);

    private static IntentFilter CloneFilter(IntentFilter filter) => new()
    {
        Field = filter.Field,
        Operator = filter.Operator,
        Value = CloneValue(filter.Value),
        ExtraFields = filter.ExtraFields == null ? null : new Dictionary<string, JsonElement>(filter.ExtraFields)
    };

    private static QueryDateRange? CloneDateRange(QueryDateRange? range) => range == null
        ? null
        : new QueryDateRange
        {
            Type = range.Type,
            Value = range.Value,
            Unit = range.Unit,
            Start = range.Start,
            End = range.End,
            DateColumn = range.DateColumn,
            Label = range.Label,
            ExtraFields = range.ExtraFields == null ? null : new Dictionary<string, JsonElement>(range.ExtraFields)
        };

    private static QuerySort CloneSort(QuerySort sort) => new()
    {
        Field = sort.Field,
        Direction = sort.Direction,
        ExtraFields = sort.ExtraFields == null ? null : new Dictionary<string, JsonElement>(sort.ExtraFields)
    };

    private static QueryComparison? CloneComparison(QueryComparison? comparison) => comparison == null
        ? null
        : new QueryComparison
        {
            Mode = comparison.Mode,
            Periods = comparison.Periods.Select(CloneDateRange).Where(range => range != null).Cast<QueryDateRange>().ToList(),
            ExtraFields = comparison.ExtraFields == null ? null : new Dictionary<string, JsonElement>(comparison.ExtraFields)
        };

    private static object? CloneValue(object? value) => value switch
    {
        null => null,
        JsonElement element => element.Clone(),
        IEnumerable<object?> objects when value is not string => objects.Select(CloneValue).ToList(),
        IEnumerable<string> strings when value is not string => strings.ToArray(),
        _ => value
    };

    private static bool LooksLikeRefinement(string userQuery, QueryIntent canonicalIntent)
    {
        if (string.IsNullOrWhiteSpace(userQuery))
            return false;

        var text = userQuery.Trim().ToLowerInvariant();
        if (text.StartsWith("а ") || text.StartsWith("теперь") || text.StartsWith("по ") || text.StartsWith("за ") || text.StartsWith("сорт") || text.StartsWith("топ"))
            return true;

        if (!string.IsNullOrWhiteSpace(canonicalIntent.Metric))
            return false;

        var words = text.Split(' ', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        return words.Length <= 6;
    }

    private static void ApplyPreviousContext(QueryIntent targetIntent, QueryIntent previousIntent)
    {
        targetIntent.Metric ??= previousIntent.Metric;
        targetIntent.Aggregation ??= previousIntent.Aggregation;
        targetIntent.Source ??= previousIntent.Source;

        if (targetIntent.Dimensions.Count == 0)
            targetIntent.Dimensions = previousIntent.Dimensions.ToList();

        if (targetIntent.Filters.Count == 0)
            targetIntent.Filters = previousIntent.Filters.Select(CloneFilter).ToList();

        targetIntent.DateRange ??= CloneDateRange(previousIntent.DateRange);
        targetIntent.Period ??= previousIntent.Period;

        if (targetIntent.Periods == null || targetIntent.Periods.Count == 0)
            targetIntent.Periods = previousIntent.Periods?.ToList();

        if (targetIntent.Sort.Count == 0)
            targetIntent.Sort = previousIntent.Sort.Select(CloneSort).ToList();

        targetIntent.Visualization ??= previousIntent.Visualization;
        targetIntent.VisualizationHint ??= previousIntent.VisualizationHint;
        targetIntent.Comparison ??= CloneComparison(previousIntent.Comparison);
    }

    private void ApplySemanticPresets(QueryIntent canonicalIntent, string userQuery)
    {
        foreach (var preset in _semanticLayer.FindPresets(userQuery))
        {
            canonicalIntent.Metric ??= preset.Metric;

            if (!string.IsNullOrWhiteSpace(preset.Dimension) &&
                canonicalIntent.Dimensions.All(dimension => !dimension.Equals(preset.Dimension, StringComparison.OrdinalIgnoreCase)))
            {
                canonicalIntent.Dimensions.Add(preset.Dimension);
            }

            foreach (var filter in preset.Filters.Select(CloneFilter))
            {
                if (string.Equals(canonicalIntent.Metric, "cancellation_rate", StringComparison.OrdinalIgnoreCase) &&
                    string.Equals(filter.Field, "status_order", StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                UpsertFilter(canonicalIntent.Filters, filter);
            }

            canonicalIntent.Visualization ??= preset.Visualization;
        }
    }

    private void ApplyQueryTextHints(QueryIntent canonicalIntent, string userQuery)
    {
        var text = userQuery.Trim().ToLowerInvariant();

        if (canonicalIntent.Limit == null)
        {
            var topMatch = System.Text.RegularExpressions.Regex.Match(text, @"(?:топ|top)\s*(\d{1,3})|^(?<leading>\d{1,3})\s+сам", RegexOptions.CultureInvariant);
            if (topMatch.Success && int.TryParse(topMatch.Groups[1].Value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var topN) && topN > 0)
                canonicalIntent.Limit = topN;
            else if (topMatch.Success && int.TryParse(topMatch.Groups["leading"].Value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var leadingTopN) && leadingTopN > 0)
                canonicalIntent.Limit = leadingTopN;
        }

        if (HasMetric("revenue_sum") &&
            (text.Contains("продаж", StringComparison.OrdinalIgnoreCase) || text.Contains("sales", StringComparison.OrdinalIgnoreCase)))
        {
            canonicalIntent.Metric = "revenue_sum";
            if (!text.Contains("отмен", StringComparison.OrdinalIgnoreCase) &&
                !text.Contains("заверш", StringComparison.OrdinalIgnoreCase) &&
                !text.Contains("completed", StringComparison.OrdinalIgnoreCase) &&
                !text.Contains("done", StringComparison.OrdinalIgnoreCase))
            {
                canonicalIntent.Filters.RemoveAll(filter => string.Equals(filter.Field, "status_order", StringComparison.OrdinalIgnoreCase));
            }

            if (IsBareSalesQuery(text))
            {
                canonicalIntent.Dimensions.Clear();
                canonicalIntent.Filters.Clear();
                canonicalIntent.Sort.Clear();
                canonicalIntent.Visualization = "table";
            }
        }

        if (HasMetric("order_price") &&
            HasDimension("order") &&
            (text.Contains("самых дорог", StringComparison.OrdinalIgnoreCase) ||
             text.Contains("самые дорог", StringComparison.OrdinalIgnoreCase) ||
             text.Contains("дорогих заказ", StringComparison.OrdinalIgnoreCase) ||
             text.Contains("expensive orders", StringComparison.OrdinalIgnoreCase)))
        {
            ForceOrderPriceList(canonicalIntent);
        }

        if (canonicalIntent.Sort.Count == 0)
        {
            if (text.Contains("по убыванию", StringComparison.OrdinalIgnoreCase) || text.Contains("descending", StringComparison.OrdinalIgnoreCase) || text.Contains("desc", StringComparison.OrdinalIgnoreCase))
            {
                canonicalIntent.Sort.Add(new QuerySort { Field = canonicalIntent.Metric ?? "metric", Direction = "desc" });
            }
            else if (text.Contains("по возрастанию", StringComparison.OrdinalIgnoreCase) || text.Contains("ascending", StringComparison.OrdinalIgnoreCase) || text.Contains("asc", StringComparison.OrdinalIgnoreCase))
            {
                canonicalIntent.Sort.Add(new QuerySort { Field = canonicalIntent.Metric ?? "metric", Direction = "asc" });
            }
        }

        if (canonicalIntent.DateRange == null && string.IsNullOrWhiteSpace(canonicalIntent.Period))
        {
            var defaultDateColumn = !string.IsNullOrWhiteSpace(canonicalIntent.Metric)
                ? _semanticLayer.ResolveMetric(canonicalIntent.Metric)?.DateColumn ?? SemanticLayer.DefaultDateColumn
                : SemanticLayer.DefaultDateColumn;

            if (_dateResolver.TryExtractDateRange(userQuery, defaultDateColumn, out var inferredDateRange))
                canonicalIntent.DateRange = inferredDateRange;
        }

        if (canonicalIntent.Intent != QueryIntentKinds.ComparePeriods &&
            (text.Contains("сравни", StringComparison.OrdinalIgnoreCase) || text.Contains("compare", StringComparison.OrdinalIgnoreCase)))
        {
            var legacyPeriods = InferLegacyComparisonPeriods(text);
            if (legacyPeriods.Count >= 2)
            {
                canonicalIntent.Intent = QueryIntentKinds.ComparePeriods;
                canonicalIntent.Periods = legacyPeriods;
            }
        }

        if (HasFilter("status_order") &&
            canonicalIntent.Filters.Count == 0 &&
            !string.Equals(canonicalIntent.Metric, "cancellation_rate", StringComparison.OrdinalIgnoreCase) &&
            text.Contains("отмен", StringComparison.OrdinalIgnoreCase))
        {
            UpsertFilter(canonicalIntent.Filters, new IntentFilter
            {
                Field = "status_order",
                Operator = "=",
                Value = "cancelled"
            });
        }

        if (HasFilter("status_order") &&
            (text.Contains("заверш", StringComparison.OrdinalIgnoreCase) || text.Contains("completed", StringComparison.OrdinalIgnoreCase) || text.Contains("done", StringComparison.OrdinalIgnoreCase)))
        {
            UpsertFilter(canonicalIntent.Filters, new IntentFilter
            {
                Field = "status_order",
                Operator = "=",
                Value = "done"
            });
        }

        if (HasFilter("price_order_local"))
        {
            AddNumericFilterFromText(
                canonicalIntent.Filters,
                text,
                "price_order_local",
                @"(?:сумм\w*|цен\w*|стоимост\w*|чек\w*|руб\w*)\D{0,24}(?<operator>больше|более|выше|дороже|от|>=|>|меньше|менее|ниже|дешевле|до|<=|<)\D{0,12}(?<value>\d+(?:[\s\u00A0]\d{3})*(?:[,.]\d+)?)");
        }

        if (HasMetric("order_price") &&
            HasDimension("order") &&
            LooksLikeOrderListWithPriceFilter(text, canonicalIntent.Filters))
        {
            ForceOrderPriceList(canonicalIntent);
        }

        if (text.Contains("pie", StringComparison.OrdinalIgnoreCase) || text.Contains("круг", StringComparison.OrdinalIgnoreCase))
            canonicalIntent.Visualization ??= "pie";
    }

    private bool HasMetric(string key) => _semanticLayer.ResolveMetric(key) != null;

    private bool HasDimension(string key) => _semanticLayer.ResolveDimension(key) != null;

    private bool HasFilter(string key) => _semanticLayer.ResolveFilter(key) != null;

    private static void ForceOrderPriceList(QueryIntent canonicalIntent)
    {
        canonicalIntent.Metric = "order_price";
        canonicalIntent.Aggregation = "max";
        canonicalIntent.Visualization = "table";
        if (canonicalIntent.Dimensions.All(dimension => !dimension.Equals("order", StringComparison.OrdinalIgnoreCase)))
            canonicalIntent.Dimensions.Add("order");
        if (canonicalIntent.Sort.Count == 0)
            canonicalIntent.Sort.Add(new QuerySort { Field = "metric", Direction = "desc" });
    }

    private static bool IsBareSalesQuery(string text)
    {
        var normalized = Regex.Replace(text, @"[^\p{L}\p{N}\s]+", " ", RegexOptions.CultureInvariant);
        normalized = Regex.Replace(normalized, @"\s+", " ", RegexOptions.CultureInvariant).Trim();

        return normalized is "продажи" or "покажи продажи" or "показать продажи" or "продажы" or "sales" or "show sales";
    }

    private static bool LooksLikeOrderListWithPriceFilter(string text, IReadOnlyList<IntentFilter> filters)
    {
        if (!filters.Any(filter => string.Equals(filter.Field, "price_order_local", StringComparison.OrdinalIgnoreCase)))
            return false;

        return (text.Contains("покажи", StringComparison.OrdinalIgnoreCase) ||
                text.Contains("показать", StringComparison.OrdinalIgnoreCase) ||
                text.Contains("show", StringComparison.OrdinalIgnoreCase)) &&
               (text.Contains("заказы", StringComparison.OrdinalIgnoreCase) ||
                text.Contains("заказов", StringComparison.OrdinalIgnoreCase) ||
                text.Contains("orders", StringComparison.OrdinalIgnoreCase));
    }

    private static void AddNumericFilterFromText(
        List<IntentFilter> filters,
        string text,
        string field,
        string pattern)
    {
        if (filters.Any(filter => string.Equals(filter.Field, field, StringComparison.OrdinalIgnoreCase)))
            return;

        var match = Regex.Match(text, pattern, RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);
        if (!match.Success)
            return;

        var valueText = match.Groups["value"].Value.Replace(" ", "", StringComparison.Ordinal).Replace("\u00A0", "", StringComparison.Ordinal);
        if (!decimal.TryParse(valueText.Replace(',', '.'), NumberStyles.Number, CultureInfo.InvariantCulture, out var value))
            return;

        var operatorText = match.Groups["operator"].Value;
        var normalizedOperator = operatorText switch
        {
            "больше" or "более" or "выше" or "дороже" or "от" or ">" => ">",
            ">=" => ">=",
            "меньше" or "менее" or "ниже" or "дешевле" or "до" or "<" => "<",
            "<=" => "<=",
            _ => ">"
        };

        filters.Add(new IntentFilter
        {
            Field = field,
            Operator = normalizedOperator,
            Value = value
        });
    }

    private static List<string> InferLegacyComparisonPeriods(string text)
    {
        if (PeriodTextPatterns.MentionsCurrentAndPreviousYear(text))
            return new List<string> { "current_year", "previous_year" };

        if (PeriodTextPatterns.MentionsCurrentAndPreviousMonth(text))
            return new List<string> { "current_month", "previous_month" };

        if (PeriodTextPatterns.MentionsCurrentAndPreviousWeek(text))
        {
            return new List<string> { "current_week", "previous_week" };
        }

        return new List<string>();
    }

    private static IEnumerable<string> BuildDimensionKeys(QueryIntent canonicalIntent)
    {
        var keys = canonicalIntent.Dimensions
            .Concat(string.IsNullOrWhiteSpace(canonicalIntent.GroupBy) ? Array.Empty<string>() : new[] { canonicalIntent.GroupBy! })
            .Where(dimension => !string.IsNullOrWhiteSpace(dimension));

        return keys.Distinct(StringComparer.OrdinalIgnoreCase);
    }

    private IEnumerable<IntentFilter> MergeFilters(MetricDefinition metricDefinition, IReadOnlyList<IntentFilter> filters)
    {
        var merged = new Dictionary<string, IntentFilter>(StringComparer.OrdinalIgnoreCase);

        foreach (var filter in metricDefinition.DefaultFilters.Select(CloneFilter))
            merged[filter.Field!] = filter;

        foreach (var filter in filters.Select(CloneFilter))
            merged[filter.Field!] = filter;

        return merged.Values;
    }

    private bool TryBuildValidatedFilter(
        IntentFilter filter,
        MetricDefinition metricDefinition,
        out ValidatedFilter? validatedFilter,
        out string? clarificationMessage)
    {
        validatedFilter = null;
        clarificationMessage = null;

        var filterDefinition = _semanticLayer.ResolveFilter(filter.Field);
        if (filterDefinition == null)
        {
            clarificationMessage = $"Неизвестный фильтр: {filter.Field}.";
            return false;
        }

        if (!metricDefinition.AllowedFilters.Contains(filterDefinition.Key))
        {
            clarificationMessage = $"Фильтр «{filterDefinition.DisplayLabel}» недоступен для метрики «{metricDefinition.DisplayLabel}».";
            return false;
        }

        var normalizedOperator = NormalizeOperator(filter.Operator);
        if (!filterDefinition.AllowedOperators.Contains(normalizedOperator))
        {
            clarificationMessage = $"Оператор `{normalizedOperator}` недоступен для фильтра `{filterDefinition.Key}`.";
            return false;
        }

        var values = IntentValueHelper.ToValues(filter.Value)
            .Select(IntentValueHelper.NormalizeScalar)
            .Where(value => value != null)
            .ToList();
        if (values.Count == 0)
        {
            clarificationMessage = $"У фильтра `{filterDefinition.Key}` отсутствует значение.";
            return false;
        }

        if (values.Count == 1 && values[0] is string singleValue && filterDefinition.ValueAliases.TryGetValue(singleValue, out var aliases))
        {
            values = aliases.Cast<object?>().ToList();
        }

        if ((normalizedOperator == "=" || normalizedOperator == "!=") && values.Count > 1)
            normalizedOperator = normalizedOperator == "=" ? "in" : "not_in";

        if ((normalizedOperator == "in" || normalizedOperator == "not_in") && values.Count == 1 && values[0] is string oneValue &&
            filterDefinition.ValueAliases.TryGetValue(oneValue, out var mappedValues))
        {
            values = mappedValues.Cast<object?>().ToList();
        }

        if (values.Count > 1 && normalizedOperator is not ("in" or "not_in"))
        {
            clarificationMessage = $"Фильтр `{filterDefinition.Key}` с несколькими значениями поддерживает только операторы `in` или `not_in`.";
            return false;
        }

        validatedFilter = new ValidatedFilter(
            filterDefinition,
            normalizedOperator,
            values,
            BuildFilterLabel(filterDefinition, normalizedOperator, values));

        return true;
    }

    private List<ValidatedSort>? BuildValidatedSorts(
        QueryIntent canonicalIntent,
        MetricDefinition metricDefinition,
        IReadOnlyList<ValidatedDimension> dimensions,
        bool isComparison,
        int limit,
        out string? clarificationMessage)
    {
        clarificationMessage = null;

        if (canonicalIntent.Sort.Count == 0)
        {
            if (isComparison)
                return new List<ValidatedSort>();

            if (dimensions.Count == 0)
                return new List<ValidatedSort>();

            if (dimensions.All(dimension => dimension.Definition.IsTimeDimension))
            {
                return dimensions
                    .Select(dimension => new ValidatedSort(dimension.Definition.Key, dimension.Alias, "asc", $"{dimension.Definition.DisplayLabel} asc"))
                    .ToList();
            }

            if (limit < _maxRows)
            {
                return new List<ValidatedSort>
                {
                    new(metricDefinition.Key, metricDefinition.Key, "desc", $"{metricDefinition.DisplayLabel} desc")
                };
            }

            return new List<ValidatedSort>
            {
                new(dimensions[0].Definition.Key, dimensions[0].Alias, "asc", $"{dimensions[0].Definition.DisplayLabel} asc")
            };
        }

        var sorts = new List<ValidatedSort>();
        foreach (var sort in canonicalIntent.Sort)
        {
            var fieldKey = TrimOrNull(sort.Field)?.ToLowerInvariant();
            if (string.IsNullOrWhiteSpace(fieldKey))
                continue;

            var direction = NormalizeDirection(sort.Direction);
            if (fieldKey == "metric" || fieldKey == metricDefinition.Key.ToLowerInvariant())
            {
                sorts.Add(new ValidatedSort(metricDefinition.Key, metricDefinition.Key, direction, $"{metricDefinition.DisplayLabel} {direction}"));
                continue;
            }

            if (isComparison && fieldKey == "period")
            {
                sorts.Add(new ValidatedSort("period", "period", direction, $"period {direction}"));
                continue;
            }

            var dimension = dimensions.FirstOrDefault(item =>
                item.Definition.Key.Equals(fieldKey, StringComparison.OrdinalIgnoreCase));
            if (dimension == null)
            {
                clarificationMessage = $"Нельзя сортировать по полю `{fieldKey}`: такого поля нет в результатах.";
                return null;
            }

            sorts.Add(new ValidatedSort(dimension.Definition.Key, dimension.Alias, direction, $"{dimension.Definition.DisplayLabel} {direction}"));
        }

        return sorts;
    }

    private static string NormalizeVisualization(
        QueryIntent canonicalIntent,
        IReadOnlyList<ValidatedDimension> dimensions,
        bool isComparison,
        string userQuery)
    {
        var visualization = TrimOrNull(canonicalIntent.Visualization ?? canonicalIntent.VisualizationHint)?.ToLowerInvariant();
        if (visualization is "bar" or "line" or "pie" or "table")
            return visualization;

        if (isComparison)
            return "bar";

        if (dimensions.Count == 0)
            return "table";

        if (dimensions.Count > 1)
            return "table";

        if (dimensions[0].Definition.IsTimeDimension)
            return "line";

        if (userQuery.Contains("доля", StringComparison.OrdinalIgnoreCase) || userQuery.Contains("share", StringComparison.OrdinalIgnoreCase))
            return "pie";

        return "bar";
    }

    private static string NormalizeOperator(string? value) => value?.Trim().ToLowerInvariant() switch
    {
        "eq" => "=",
        "ne" => "!=",
        "in" => "in",
        "not in" => "not_in",
        "not_in" => "not_in",
        "gte" => ">=",
        "lte" => "<=",
        "gt" => ">",
        "lt" => "<",
        "=" or "!=" or ">" or ">=" or "<" or "<=" => value.Trim().ToLowerInvariant(),
        _ => "="
    };

    private static string NormalizeDirection(string? value) => value?.Trim().ToLowerInvariant() switch
    {
        "descending" => "desc",
        "ascending" => "asc",
        "desc" => "desc",
        _ => "asc"
    };

    private static string BuildFilterLabel(FilterDefinition filterDefinition, string normalizedOperator, IReadOnlyList<object?> values)
    {
        var renderedValue = string.Join(", ", values.Select(IntentValueHelper.ToDisplayString));
        var operatorLabel = normalizedOperator switch
        {
            "=" => "=",
            "!=" => "!=",
            "in" => "in",
            "not_in" => "not in",
            ">" => ">",
            ">=" => ">=",
            "<" => "<",
            "<=" => "<=",
            _ => normalizedOperator
        };

        return $"{filterDefinition.DisplayLabel} {operatorLabel} {renderedValue}";
    }

    private static bool NeedsPeriodClarification(
        string userQuery,
        ResolvedDateRange? dateRange,
        IReadOnlyList<ResolvedDateRange> comparisonRanges,
        IReadOnlyList<ValidatedDimension> dimensions,
        IReadOnlyList<ValidatedFilter> filters)
    {
        if (dateRange != null || comparisonRanges.Count > 0)
            return false;

        if (dimensions.Count > 0 || filters.Count > 0)
            return false;

        if (string.IsNullOrWhiteSpace(userQuery))
            return false;

        if (userQuery.Contains("все время", StringComparison.OrdinalIgnoreCase) ||
            userQuery.Contains("за всё время", StringComparison.OrdinalIgnoreCase) ||
            userQuery.Contains("overall", StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        var words = userQuery.Split(' ', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        return words.Length <= 4;
    }

    private static QueryDateRange? CanonicalizeDateRange(QueryDateRange? source, ResolvedDateRange? resolved)
    {
        if (resolved == null)
            return source == null ? null : CloneDateRange(source);

        return new QueryDateRange
        {
            Type = source?.Type,
            Value = source?.Value,
            Unit = source?.Unit,
            Start = source?.Start,
            End = source?.End,
            DateColumn = resolved.DateColumn,
            Label = resolved.Label
        };
    }

    private static QueryComparison? BuildComparison(QueryIntent sourceIntent, IReadOnlyList<ResolvedDateRange> ranges, string defaultDateColumn)
    {
        if (sourceIntent.Comparison == null && (sourceIntent.Periods == null || sourceIntent.Periods.Count == 0))
            return null;

        var comparison = CloneComparison(sourceIntent.Comparison) ?? new QueryComparison();
        if (comparison.Periods.Count == 0)
        {
            comparison.Periods = ranges
                .Select(range => new QueryDateRange
                {
                    Type = "absolute",
                    Start = range.FromUtc.ToString("yyyy-MM-dd"),
                    End = range.ToExclusiveUtc.AddDays(-1).ToString("yyyy-MM-dd"),
                    DateColumn = defaultDateColumn,
                    Label = range.Label
                })
                .ToList();
        }

        return comparison;
    }

    private static string? BuildLegacyPeriod(QueryDateRange? normalizedIntentDateRange)
    {
        if (normalizedIntentDateRange == null || string.IsNullOrWhiteSpace(normalizedIntentDateRange.Type))
            return null;

        return normalizedIntentDateRange.Type.Trim().ToLowerInvariant() switch
        {
            "today" => "today",
            "yesterday" => "yesterday",
            "current_week" => "current_week",
            "previous_week" => "previous_week",
            "current_month" => "current_month",
            "previous_month" => "previous_month",
            "current_year" => "current_year",
            "previous_year" => "previous_year",
            "last_n_days" when normalizedIntentDateRange.Value.HasValue => $"last_{normalizedIntentDateRange.Value.Value}_days",
            "last_n_weeks" when normalizedIntentDateRange.Value.HasValue => $"last_{normalizedIntentDateRange.Value.Value}_weeks",
            "last_n_months" when normalizedIntentDateRange.Value.HasValue => $"last_{normalizedIntentDateRange.Value.Value}_months",
            _ => null
        };
    }

    private static List<string>? BuildLegacyPeriods(QueryIntent canonicalIntent, IReadOnlyList<ResolvedDateRange> ranges, string defaultDateColumn)
    {
        if (canonicalIntent.Periods?.Count > 0)
            return canonicalIntent.Periods.ToList();

        if (canonicalIntent.Comparison?.Periods.Count > 0)
        {
            return canonicalIntent.Comparison.Periods
                .Select(period => BuildLegacyPeriod(new QueryDateRange
                {
                    Type = period.Type,
                    Value = period.Value,
                    Unit = period.Unit,
                    DateColumn = period.DateColumn ?? defaultDateColumn
                }))
                .Where(period => !string.IsNullOrWhiteSpace(period))
                .Cast<string>()
                .ToList();
        }

        return ranges.Count == 0 ? null : ranges.Select(range => range.Label).ToList();
    }

    private static IntentFilter ToIntentFilter(ValidatedFilter filter)
    {
        object? value = filter.Values.Count switch
        {
            0 => null,
            1 => filter.Values[0],
            _ => filter.Values.Select(IntentValueHelper.NormalizeScalar).ToArray()
        };

        return new IntentFilter
        {
            Field = filter.Definition.Key,
            Operator = filter.Operator,
            Value = value
        };
    }

    private static void UpsertFilter(ICollection<IntentFilter> filters, IntentFilter filter)
    {
        var existing = filters.FirstOrDefault(item =>
            item.Field != null &&
            filter.Field != null &&
            item.Field.Equals(filter.Field, StringComparison.OrdinalIgnoreCase));

        if (existing != null)
            filters.Remove(existing);

        filters.Add(filter);
    }

    private static bool HasUnexpectedFields(QueryIntent intent, out List<string> unexpectedFields)
    {
        unexpectedFields = new List<string>();

        if (intent.ExtraFields?.Count > 0)
            unexpectedFields.AddRange(intent.ExtraFields.Keys.Select(key => $"intent.{key}"));

        foreach (var filter in intent.Filters.Where(filter => filter.ExtraFields?.Count > 0))
            unexpectedFields.AddRange(filter.ExtraFields!.Keys.Select(key => $"filter.{key}"));

        if (intent.DateRange?.ExtraFields?.Count > 0)
            unexpectedFields.AddRange(intent.DateRange.ExtraFields.Keys.Select(key => $"date_range.{key}"));

        foreach (var sort in intent.Sort.Where(sort => sort.ExtraFields?.Count > 0))
            unexpectedFields.AddRange(sort.ExtraFields!.Keys.Select(key => $"sort.{key}"));

        if (intent.Comparison?.ExtraFields?.Count > 0)
            unexpectedFields.AddRange(intent.Comparison.ExtraFields.Keys.Select(key => $"comparison.{key}"));

        foreach (var period in intent.Comparison?.Periods.Where(period => period.ExtraFields?.Count > 0) ?? Array.Empty<QueryDateRange>())
            unexpectedFields.AddRange(period.ExtraFields!.Keys.Select(key => $"comparison.periods.{key}"));

        return unexpectedFields.Count > 0;
    }

    private static string BuildDiscriminatorKey(QueryIntent normalizedIntent, IReadOnlyList<ResolvedDateRange> comparisonRanges)
    {
        var dimensions = string.Join(",", normalizedIntent.Dimensions.OrderBy(value => value, StringComparer.OrdinalIgnoreCase));
        var filters = string.Join(
            ";",
            normalizedIntent.Filters
                .OrderBy(filter => filter.Field, StringComparer.OrdinalIgnoreCase)
                .ThenBy(filter => filter.Operator, StringComparer.OrdinalIgnoreCase)
                .Select(filter =>
                    $"{filter.Field}:{filter.Operator}:{string.Join(",", IntentValueHelper.ToValues(filter.Value).Select(IntentValueHelper.ToDisplayString))}"));
        var sorts = string.Join(
            ";",
            normalizedIntent.Sort
                .OrderBy(sort => sort.Field, StringComparer.OrdinalIgnoreCase)
                .Select(sort => $"{sort.Field}:{sort.Direction}"));
        var comparison = comparisonRanges.Count == 0
            ? normalizedIntent.DateRange == null
                ? "-"
                : $"{normalizedIntent.DateRange.Type}:{normalizedIntent.DateRange.Value}:{normalizedIntent.DateRange.Start}:{normalizedIntent.DateRange.End}:{normalizedIntent.DateRange.DateColumn}"
            : string.Join("||", comparisonRanges.Select(range => $"{range.FromUtc:O}->{range.ToExclusiveUtc:O}@{range.DateColumn}"));

        return string.Join(
            "|",
            normalizedIntent.Intent,
            normalizedIntent.Source ?? "-",
            normalizedIntent.Metric ?? "-",
            normalizedIntent.Aggregation ?? "-",
            dimensions,
            filters,
            comparison,
            sorts,
            normalizedIntent.Limit?.ToString(CultureInfo.InvariantCulture) ?? "-");
    }
}
