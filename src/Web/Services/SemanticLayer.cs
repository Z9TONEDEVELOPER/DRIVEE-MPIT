namespace DriveeDataSpace.Web.Services;

public record MetricDef(
    string Key,
    string[] Synonyms,
    string DisplayName,
    string Aggregation,
    string Expression,
    string Table,
    string? Filter = null
);

public record DimensionDef(string Key, string[] Synonyms, string Sql);

public class SemanticLayer
{
    public const string OrdersTable = "orders";
    public const string TimestampColumn = "order_timestamp";

    public IReadOnlyList<MetricDef> Metrics { get; }
    public IReadOnlyList<DimensionDef> Dimensions { get; }

    public SemanticLayer()
    {
        const string doneFilter = "status_order = 'done'";
        const string cancelFilter = "status_order IN ('cancel','delete')";

        Metrics = new List<MetricDef>
        {
            new("orders",
                new[] { "заказы", "поездки", "поездок", "заказов", "orders", "rides", "trips" },
                "Завершённые заказы",
                "COUNT", "*", OrdersTable, doneFilter),

            new("revenue",
                new[] { "выручка", "выручку", "revenue", "доход" },
                "Выручка, ₽",
                "SUM", "price_order_local", OrdersTable, doneFilter),

            new("avg_check",
                new[] { "средний чек", "avg_check", "средняя стоимость" },
                "Средний чек, ₽",
                "AVG", "price_order_local", OrdersTable, doneFilter),

            new("duration",
                new[] { "длительность", "длительности", "duration", "продолжительность" },
                "Средняя длительность, мин",
                "AVG", "duration_in_seconds / 60.0", OrdersTable, doneFilter),

            new("distance",
                new[] { "расстояние", "дистанция", "distance", "км", "километры" },
                "Среднее расстояние, км",
                "AVG", "distance_in_meters / 1000.0", OrdersTable, doneFilter),

            new("cancellations",
                new[] { "отмены", "отмен", "cancellations", "cancelled", "cancel" },
                "Отмены",
                "COUNT", "*", OrdersTable, cancelFilter),

            new("tenders_per_order",
                new[] { "тендеры на заказ", "тендеров на заказ", "tenders_per_order" },
                "Тендеров на заказ (ср.)",
                "AVG", "tender_count", OrdersTable, null),

            new("cancellation_rate",
                new[] { "процент отмен", "cancellation_rate", "доля отмен" },
                "Доля отмен, %",
                "ROUND",
                "100.0 * SUM(CASE WHEN status_order IN ('cancel','delete') THEN 1 ELSE 0 END) / COUNT(*), 2",
                OrdersTable, null),
        };

        Dimensions = new List<DimensionDef>
        {
            new("day",     new[] { "день", "дню", "дата", "day", "date" },  "date(order_timestamp)"),
            new("week",    new[] { "неделя", "неделе", "week" },             "strftime('%Y-W%W', order_timestamp)"),
            new("month",   new[] { "месяц", "месяцу", "month" },             "strftime('%Y-%m', order_timestamp)"),
            new("hour",    new[] { "час", "часам", "hour" },                 "CAST(strftime('%H', order_timestamp) AS INTEGER)"),
            new("weekday", new[] { "день недели", "weekday" },               "CAST(strftime('%w', order_timestamp) AS INTEGER)"),
            new("status",  new[] { "статус", "status", "status_order" },     "status_order"),
            new("city",    new[] { "город", "городам", "city", "city_id" },  "city_id"),
        };
    }

    public MetricDef? ResolveMetric(string? key)
    {
        if (string.IsNullOrWhiteSpace(key)) return null;
        var k = key.Trim().ToLowerInvariant();
        return Metrics.FirstOrDefault(m => m.Key == k)
            ?? Metrics.FirstOrDefault(m => m.Synonyms.Any(s => s.ToLowerInvariant() == k));
    }

    public DimensionDef? ResolveDimension(string? key)
    {
        if (string.IsNullOrWhiteSpace(key)) return null;
        var k = key.Trim().ToLowerInvariant();
        return Dimensions.FirstOrDefault(d => d.Key == k)
            ?? Dimensions.FirstOrDefault(d => d.Synonyms.Any(s => s.ToLowerInvariant() == k));
    }
}
