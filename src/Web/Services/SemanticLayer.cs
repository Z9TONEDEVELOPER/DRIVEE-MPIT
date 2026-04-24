using DriveeDataSpace.Web.Models;

namespace DriveeDataSpace.Web.Services;

public sealed record SourceDefinition(
    string Key,
    string Table,
    string DisplayName,
    IReadOnlySet<string> AllowedColumns);

public sealed record MetricDefinition(
    string Key,
    string DisplayLabel,
    string Aggregation,
    string Expression,
    string Source,
    string DateColumn,
    IReadOnlySet<string> AllowedDimensions,
    IReadOnlySet<string> AllowedFilters,
    IReadOnlyList<IntentFilter> DefaultFilters,
    string[] Synonyms);

public sealed record DimensionDefinition(
    string Key,
    string DisplayLabel,
    string ExpressionTemplate,
    bool IsTimeDimension,
    string Source,
    string[] Synonyms);

public sealed record FilterDefinition(
    string Key,
    string DisplayLabel,
    string Column,
    string Source,
    IReadOnlySet<string> AllowedOperators,
    IReadOnlyDictionary<string, IReadOnlyList<string>> ValueAliases,
    string[] Synonyms);

public sealed record SemanticPreset(
    string Key,
    string[] Phrases,
    string? Metric,
    string? Dimension,
    IReadOnlyList<IntentFilter> Filters,
    string? Visualization);

public class SemanticLayer
{
    public const string OrdersSource = "orders";
    public const string OrdersTable = "orders";
    public const string DefaultDateColumn = "order_timestamp";

    public IReadOnlyList<SourceDefinition> Sources { get; }
    public IReadOnlyList<MetricDefinition> Metrics { get; }
    public IReadOnlyList<DimensionDefinition> Dimensions { get; }
    public IReadOnlyList<FilterDefinition> Filters { get; }
    public IReadOnlyList<SemanticPreset> Presets { get; }

    public SemanticLayer()
    {
        Sources = new List<SourceDefinition>
        {
            new(
                OrdersSource,
                OrdersTable,
                "Заказы",
                new HashSet<string>(StringComparer.OrdinalIgnoreCase)
                {
                    "order_id",
                    "city_id",
                    "status_order",
                    "order_timestamp",
                    "driverdone_timestamp",
                    "distance_in_meters",
                    "duration_in_seconds",
                    "price_order_local",
                    "price_start_local",
                    "tender_count",
                    "accepts"
                }
            )
        };

        var commonDimensions = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            "order",
            "day",
            "week",
            "month",
            "hour",
            "weekday",
            "city",
            "status_order"
        };

        var commonFilters = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            "status_order",
            "city",
            "price_order_local",
            "duration_in_seconds",
            "distance_in_meters",
            "tender_count"
        };

        Metrics = new List<MetricDefinition>
        {
            new(
                "orders_count",
                "Количество заказов",
                "count",
                "order_id",
                OrdersSource,
                DefaultDateColumn,
                commonDimensions,
                commonFilters,
                Array.Empty<IntentFilter>(),
                new[]
                {
                    "orders_count", "orders", "заказы", "заказов", "поездки", "поездок", "поездка", "заявки", "rides", "trips"
                }),
            new(
                "revenue_sum",
                "Выручка",
                "sum",
                "price_order_local",
                OrdersSource,
                DefaultDateColumn,
                commonDimensions,
                commonFilters,
                new[]
                {
                    new IntentFilter { Field = "status_order", Operator = "=", Value = "done" }
                },
                new[]
                {
                    "revenue_sum", "revenue", "выручка", "доход", "gmv", "продажи", "продаж", "sales"
                }),
            new(
                "order_price",
                "Стоимость заказа",
                "max",
                "price_order_local",
                OrdersSource,
                DefaultDateColumn,
                commonDimensions,
                commonFilters,
                new[]
                {
                    new IntentFilter { Field = "status_order", Operator = "=", Value = "done" }
                },
                new[]
                {
                    "order_price", "стоимость заказа", "цена заказа", "сумма заказа", "дорогие заказы", "самые дорогие заказы"
                }),
            new(
                "avg_order_price",
                "Средний чек",
                "avg",
                "price_order_local",
                OrdersSource,
                DefaultDateColumn,
                commonDimensions,
                commonFilters,
                new[]
                {
                    new IntentFilter { Field = "status_order", Operator = "=", Value = "done" }
                },
                new[]
                {
                    "avg_order_price", "avg_check", "average_order_price", "средний чек", "средняя стоимость", "средняя цена заказа"
                }),
            new(
                "avg_trip_duration",
                "Средняя длительность поездки",
                "avg",
                "duration_in_seconds / 60.0",
                OrdersSource,
                DefaultDateColumn,
                commonDimensions,
                commonFilters,
                new[]
                {
                    new IntentFilter { Field = "status_order", Operator = "=", Value = "done" }
                },
                new[]
                {
                    "avg_trip_duration", "duration", "длительность", "длительность поездки", "продолжительность поездки"
                }),
            new(
                "avg_distance_km",
                "Средняя дистанция",
                "avg",
                "distance_in_meters / 1000.0",
                OrdersSource,
                DefaultDateColumn,
                commonDimensions,
                commonFilters,
                new[]
                {
                    new IntentFilter { Field = "status_order", Operator = "=", Value = "done" }
                },
                new[]
                {
                    "avg_distance_km", "distance", "расстояние", "дистанция", "километры", "км"
                }),
            new(
                "cancellation_rate",
                "Доля отмен",
                "formula",
                "ROUND(100.0 * SUM(CASE WHEN status_order IN ('cancel', 'delete') THEN 1 ELSE 0 END) / NULLIF(COUNT(order_id), 0), 2)",
                OrdersSource,
                DefaultDateColumn,
                commonDimensions,
                commonFilters,
                Array.Empty<IntentFilter>(),
                new[]
                {
                    "cancellation_rate", "доля отмен", "процент отмен", "cancel rate"
                }),
            new(
                "tenders_per_order",
                "Тендеров на заказ",
                "avg",
                "tender_count",
                OrdersSource,
                DefaultDateColumn,
                commonDimensions,
                commonFilters,
                Array.Empty<IntentFilter>(),
                new[]
                {
                    "tenders_per_order", "тендеров на заказ", "тендеры", "tenders"
                })
        };

        Dimensions = new List<DimensionDefinition>
        {
            new("order", "по заказам", "order_id", false, OrdersSource, new[] { "order", "order_id", "заказ", "заказам", "по заказам" }),
            new("day", "по дням", "date({date_column})", true, OrdersSource, new[] { "day", "date", "день", "дням", "по дням", "дата" }),
            new("week", "по неделям", "strftime('%Y-W%W', {date_column})", true, OrdersSource, new[] { "week", "неделя", "неделям", "по неделям" }),
            new("month", "по месяцам", "strftime('%Y-%m', {date_column})", true, OrdersSource, new[] { "month", "месяц", "месяцам", "по месяцам" }),
            new("hour", "по часам", "CAST(strftime('%H', {date_column}) AS INTEGER)", true, OrdersSource, new[] { "hour", "час", "часам", "по часам" }),
            new("weekday", "по дням недели", "CAST(strftime('%w', {date_column}) AS INTEGER)", true, OrdersSource, new[] { "weekday", "день недели", "дням недели" }),
            new("city", "по городам", "city_id", false, OrdersSource, new[] { "city", "город", "городу", "городам", "по городам", "city_id" }),
            new("status_order", "по статусам", "status_order", false, OrdersSource, new[] { "status_order", "status", "статус", "статус заказа", "по статусам" })
        };

        Filters = new List<FilterDefinition>
        {
            new(
                "status_order",
                "статус заказа",
                "status_order",
                OrdersSource,
                new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "=", "!=", "in", "not_in" },
                new Dictionary<string, IReadOnlyList<string>>(StringComparer.OrdinalIgnoreCase)
                {
                    ["done"] = new[] { "done" },
                    ["completed"] = new[] { "done" },
                    ["завершен"] = new[] { "done" },
                    ["завершён"] = new[] { "done" },
                    ["завершенные"] = new[] { "done" },
                    ["завершённые"] = new[] { "done" },
                    ["completed_orders"] = new[] { "done" },
                    ["cancelled"] = new[] { "cancel", "delete" },
                    ["canceled"] = new[] { "cancel", "delete" },
                    ["cancel"] = new[] { "cancel" },
                    ["delete"] = new[] { "delete" },
                    ["отменен"] = new[] { "cancel", "delete" },
                    ["отменён"] = new[] { "cancel", "delete" },
                    ["отмененные"] = new[] { "cancel", "delete" },
                    ["отменённые"] = new[] { "cancel", "delete" },
                    ["отмена"] = new[] { "cancel", "delete" }
                },
                new[] { "status_order", "status", "статус", "статус заказа" }),
            new(
                "city",
                "город",
                "city_id",
                OrdersSource,
                new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "=", "!=", "in", "not_in" },
                new Dictionary<string, IReadOnlyList<string>>(StringComparer.OrdinalIgnoreCase),
                new[] { "city", "город", "города", "по городу", "city_id" }),
            new(
                "price_order_local",
                "стоимость заказа",
                "price_order_local",
                OrdersSource,
                new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "=", "!=", ">", ">=", "<", "<=" },
                new Dictionary<string, IReadOnlyList<string>>(StringComparer.OrdinalIgnoreCase),
                new[] { "price_order_local", "цена", "стоимость", "чек", "сумма заказа" }),
            new(
                "duration_in_seconds",
                "длительность",
                "duration_in_seconds",
                OrdersSource,
                new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "=", "!=", ">", ">=", "<", "<=" },
                new Dictionary<string, IReadOnlyList<string>>(StringComparer.OrdinalIgnoreCase),
                new[] { "duration_in_seconds", "длительность", "продолжительность" }),
            new(
                "distance_in_meters",
                "расстояние",
                "distance_in_meters",
                OrdersSource,
                new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "=", "!=", ">", ">=", "<", "<=" },
                new Dictionary<string, IReadOnlyList<string>>(StringComparer.OrdinalIgnoreCase),
                new[] { "distance_in_meters", "расстояние", "дистанция" }),
            new(
                "tender_count",
                "количество тендеров",
                "tender_count",
                OrdersSource,
                new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "=", "!=", ">", ">=", "<", "<=" },
                new Dictionary<string, IReadOnlyList<string>>(StringComparer.OrdinalIgnoreCase),
                new[] { "tender_count", "тендеры", "тендеров" })
        };

        Presets = new List<SemanticPreset>
        {
            new(
                "completed_orders",
                new[] { "завершенные заказы", "завершённые заказы", "completed orders", "done orders", "завершенные поездки", "завершённые поездки" },
                "orders_count",
                null,
                new[]
                {
                    new IntentFilter { Field = "status_order", Operator = "=", Value = "done" }
                },
                null),
            new(
                "cancelled_orders",
                new[] { "отмененные заказы", "отменённые заказы", "cancelled orders", "canceled orders", "отмены" },
                "orders_count",
                null,
                new[]
                {
                    new IntentFilter { Field = "status_order", Operator = "=", Value = "cancelled" }
                },
                null),
            new(
                "daily_breakdown",
                new[] { "по дням", "динамика по дням" },
                null,
                "day",
                Array.Empty<IntentFilter>(),
                "line"),
            new(
                "city_breakdown",
                new[] { "по городам" },
                null,
                "city",
                Array.Empty<IntentFilter>(),
                "bar"),
            new(
                "status_breakdown",
                new[] { "по статусам" },
                null,
                "status_order",
                Array.Empty<IntentFilter>(),
                "bar")
        };
    }

    public MetricDefinition? ResolveMetric(string? key)
    {
        if (string.IsNullOrWhiteSpace(key))
            return null;

        return Metrics.FirstOrDefault(metric =>
            metric.Key.Equals(key.Trim(), StringComparison.OrdinalIgnoreCase) ||
            metric.Synonyms.Any(synonym => synonym.Equals(key.Trim(), StringComparison.OrdinalIgnoreCase)));
    }

    public MetricDefinition? MatchMetricInText(string? text) =>
        Metrics
            .Select(metric => new
            {
                Metric = metric,
                MatchLength = metric.Synonyms
                    .Where(synonym => ContainsPhrase(text, synonym))
                    .Select(synonym => synonym.Length)
                    .DefaultIfEmpty(0)
                    .Max()
            })
            .Where(candidate => candidate.MatchLength > 0)
            .OrderByDescending(candidate => candidate.MatchLength)
            .Select(candidate => candidate.Metric)
            .FirstOrDefault();

    public DimensionDefinition? ResolveDimension(string? key)
    {
        if (string.IsNullOrWhiteSpace(key))
            return null;

        return Dimensions.FirstOrDefault(dimension =>
            dimension.Key.Equals(key.Trim(), StringComparison.OrdinalIgnoreCase) ||
            dimension.Synonyms.Any(synonym => synonym.Equals(key.Trim(), StringComparison.OrdinalIgnoreCase)));
    }

    public DimensionDefinition? MatchDimensionInText(string? text) =>
        Dimensions
            .Select(dimension => new
            {
                Dimension = dimension,
                MatchLength = dimension.Synonyms
                    .Where(synonym => ContainsPhrase(text, synonym))
                    .Select(synonym => synonym.Length)
                    .DefaultIfEmpty(0)
                    .Max()
            })
            .Where(candidate => candidate.MatchLength > 0)
            .OrderByDescending(candidate => candidate.MatchLength)
            .Select(candidate => candidate.Dimension)
            .FirstOrDefault();

    public FilterDefinition? ResolveFilter(string? key)
    {
        if (string.IsNullOrWhiteSpace(key))
            return null;

        return Filters.FirstOrDefault(filter =>
            filter.Key.Equals(key.Trim(), StringComparison.OrdinalIgnoreCase) ||
            filter.Synonyms.Any(synonym => synonym.Equals(key.Trim(), StringComparison.OrdinalIgnoreCase)));
    }

    public SourceDefinition? ResolveSource(string? key)
    {
        if (string.IsNullOrWhiteSpace(key))
            return null;

        return Sources.FirstOrDefault(source =>
            source.Key.Equals(key.Trim(), StringComparison.OrdinalIgnoreCase) ||
            source.Table.Equals(key.Trim(), StringComparison.OrdinalIgnoreCase));
    }

    public IReadOnlyList<SemanticPreset> FindPresets(string? userQuery) =>
        Presets
            .Where(preset => preset.Phrases.Any(phrase => ContainsPhrase(userQuery, phrase)))
            .OrderByDescending(preset => preset.Phrases.Max(phrase => phrase.Length))
            .ToList();

    public string RenderDimensionSql(DimensionDefinition dimension, string dateColumn) =>
        dimension.ExpressionTemplate.Replace("{date_column}", dateColumn, StringComparison.OrdinalIgnoreCase);

    private static bool ContainsPhrase(string? text, string phrase) =>
        !string.IsNullOrWhiteSpace(text) &&
        !string.IsNullOrWhiteSpace(phrase) &&
        text.Contains(phrase, StringComparison.OrdinalIgnoreCase);
}
