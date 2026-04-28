namespace NexusDataSpace.Core.Services;

public sealed record FewShotExample(string Query, string IntentJson);

public interface IFewShotProvider
{
    IReadOnlyList<FewShotExample> SelectFor(string userQuery, int max);
}

public sealed class StaticFewShotProvider : IFewShotProvider
{
    private static readonly IReadOnlyList<FewShotExample> Examples = new List<FewShotExample>
    {
        new(
            "Сколько заказов сегодня?",
            "{\"kind\":\"query\",\"intent\":\"metric_query\",\"metric\":\"orders_count\",\"aggregation\":\"count\",\"dimensions\":[],\"filters\":[],\"date_range\":{\"type\":\"today\",\"date_column\":\"order_timestamp\"},\"sort\":[],\"limit\":null,\"visualization\":\"table\",\"source\":\"orders\",\"confidence\":0.95}"),

        new(
            "Выручка по городам за прошлую неделю",
            "{\"kind\":\"query\",\"intent\":\"metric_query\",\"metric\":\"revenue_sum\",\"aggregation\":\"sum\",\"dimensions\":[\"city\"],\"filters\":[],\"date_range\":{\"type\":\"previous_week\",\"date_column\":\"order_timestamp\"},\"sort\":[{\"field\":\"revenue_sum\",\"direction\":\"desc\"}],\"limit\":null,\"visualization\":\"bar\",\"source\":\"orders\",\"confidence\":0.92}"),

        new(
            "Топ 5 городов по выручке за этот месяц",
            "{\"kind\":\"query\",\"intent\":\"metric_query\",\"metric\":\"revenue_sum\",\"aggregation\":\"sum\",\"dimensions\":[\"city\"],\"filters\":[],\"date_range\":{\"type\":\"current_month\",\"date_column\":\"order_timestamp\"},\"sort\":[{\"field\":\"revenue_sum\",\"direction\":\"desc\"}],\"limit\":5,\"visualization\":\"bar\",\"source\":\"orders\",\"confidence\":0.95}"),

        new(
            "Сколько отменённых заказов за последние 7 дней?",
            "{\"kind\":\"query\",\"intent\":\"metric_query\",\"metric\":\"orders_count\",\"aggregation\":\"count\",\"dimensions\":[],\"filters\":[{\"field\":\"status_order\",\"operator\":\"=\",\"value\":\"cancelled\"}],\"date_range\":{\"type\":\"last_n_days\",\"value\":7,\"date_column\":\"order_timestamp\"},\"sort\":[],\"limit\":null,\"visualization\":\"table\",\"source\":\"orders\",\"confidence\":0.93}"),

        new(
            "Динамика заказов по дням за этот месяц",
            "{\"kind\":\"query\",\"intent\":\"metric_query\",\"metric\":\"orders_count\",\"aggregation\":\"count\",\"dimensions\":[\"day\"],\"filters\":[],\"date_range\":{\"type\":\"current_month\",\"date_column\":\"order_timestamp\"},\"sort\":[],\"limit\":null,\"visualization\":\"line\",\"source\":\"orders\",\"confidence\":0.94}"),

        new(
            "Сравни выручку этого и прошлого месяца",
            "{\"kind\":\"query\",\"intent\":\"compare_periods\",\"metric\":\"revenue_sum\",\"aggregation\":\"sum\",\"dimensions\":[],\"filters\":[],\"date_range\":null,\"sort\":[],\"limit\":null,\"visualization\":\"bar\",\"source\":\"orders\",\"comparison\":{\"mode\":\"period_vs_period\",\"periods\":[{\"type\":\"current_month\",\"date_column\":\"order_timestamp\"},{\"type\":\"previous_month\",\"date_column\":\"order_timestamp\"}]},\"confidence\":0.9}"),

        new(
            "Средний чек по городам за вчера",
            "{\"kind\":\"query\",\"intent\":\"metric_query\",\"metric\":\"avg_order_price\",\"aggregation\":\"avg\",\"dimensions\":[\"city\"],\"filters\":[],\"date_range\":{\"type\":\"yesterday\",\"date_column\":\"order_timestamp\"},\"sort\":[{\"field\":\"avg_order_price\",\"direction\":\"desc\"}],\"limit\":null,\"visualization\":\"bar\",\"source\":\"orders\",\"confidence\":0.9}"),

        new(
            "Покажи статистику",
            "{\"kind\":\"clarify\",\"clarification\":\"Уточните, какую метрику показать (заказы, выручка, средний чек и т.д.) и за какой период.\",\"intent\":\"metric_query\",\"metric\":null,\"aggregation\":null,\"dimensions\":[],\"filters\":[],\"date_range\":null,\"sort\":[],\"limit\":null,\"visualization\":null,\"source\":null,\"confidence\":0.3}")
    };

    public IReadOnlyList<FewShotExample> SelectFor(string userQuery, int max)
    {
        if (max <= 0)
            return Array.Empty<FewShotExample>();

        return max >= Examples.Count
            ? Examples
            : Examples.Take(max).ToList();
    }
}
