using System.Text;
using DriveeDataSpace.Web.Models;

namespace DriveeDataSpace.Web.Services;

public record BuiltSql(string Sql, Dictionary<string, object?> Parameters, string HumanExplain, string TechExplain);

public class SqlBuilder
{
    private readonly SemanticLayer _semantic;
    private readonly int _maxRows;

    public SqlBuilder(SemanticLayer semantic, IConfiguration cfg)
    {
        _semantic = semantic;
        _maxRows = int.TryParse(cfg["Data:MaxRows"], out var m) ? m : 10000;
    }

    public BuiltSql Build(QueryIntent intent)
    {
        var metric = _semantic.ResolveMetric(intent.Metric)
                     ?? throw new InvalidOperationException($"Неизвестная метрика: {intent.Metric}");

        if (intent.Intent == "compare_periods" && intent.Periods != null && intent.Periods.Count >= 2)
            return BuildCompare(metric, intent);

        return BuildAggregate(metric, intent);
    }

    private BuiltSql BuildAggregate(MetricDef metric, QueryIntent intent)
    {
        var dim = _semantic.ResolveDimension(intent.GroupBy);
        var period = PeriodResolver.Resolve(intent.Period);
        var pars = new Dictionary<string, object?>();

        var sb = new StringBuilder();
        sb.Append("SELECT ");
        if (dim != null)
            sb.Append($"{dim.Sql} AS \"{dim.Key}\", ");
        sb.Append($"{metric.Aggregation}({metric.Expression}) AS \"{metric.Key}\" ");
        sb.Append($"FROM {metric.Table} ");

        var where = new List<string>();
        if (!string.IsNullOrEmpty(metric.Filter)) where.Add(metric.Filter);
        if (period != null)
        {
            where.Add("order_timestamp >= $from AND order_timestamp < $to");
            pars["$from"] = period.From.ToString("yyyy-MM-dd HH:mm:ss");
            pars["$to"] = period.To.ToString("yyyy-MM-dd HH:mm:ss");
        }
        ApplyFilters(intent.Filters, where, pars);
        if (where.Count > 0) sb.Append("WHERE ").Append(string.Join(" AND ", where)).Append(' ');

        if (dim != null)
            sb.Append($"GROUP BY {dim.Sql} ORDER BY {dim.Sql} ");
        sb.Append($"LIMIT {_maxRows}");

        var humanParts = new List<string> { $"Посчитана метрика «{metric.DisplayName}»" };
        if (dim != null) humanParts.Add($"с группировкой по «{dim.Key}»");
        if (period != null) humanParts.Add($"за период: {period.Label}");
        ApplyFiltersHuman(intent.Filters, humanParts);

        var tech = $"Шаблон: aggregate | метрика={metric.Key} ({metric.Aggregation}({metric.Expression})) | " +
                   $"group_by={dim?.Key ?? "-"} | period={intent.Period ?? "-"} | filters={FormatFilters(intent.Filters)}";

        return new BuiltSql(sb.ToString(), pars, string.Join(", ", humanParts) + ".", tech);
    }

    private BuiltSql BuildCompare(MetricDef metric, QueryIntent intent)
    {
        var pars = new Dictionary<string, object?>();
        var selects = new List<string>();
        var humanPeriods = new List<string>();
        var i = 0;
        foreach (var pKey in intent.Periods!)
        {
            var p = PeriodResolver.Resolve(pKey);
            if (p == null) continue;
            var fromKey = $"$f{i}";
            var toKey = $"$t{i}";
            pars[fromKey] = p.From.ToString("yyyy-MM-dd HH:mm:ss");
            pars[toKey] = p.To.ToString("yyyy-MM-dd HH:mm:ss");

            var filter = string.IsNullOrEmpty(metric.Filter) ? "" : $"{metric.Filter} AND ";
            selects.Add(
                $"SELECT '{p.Label}' AS period, {metric.Aggregation}({metric.Expression}) AS \"{metric.Key}\" " +
                $"FROM {metric.Table} WHERE {filter}order_timestamp >= {fromKey} AND order_timestamp < {toKey}"
            );
            humanPeriods.Add(p.Label);
            i++;
        }

        if (selects.Count < 2)
            throw new InvalidOperationException("Для сравнения нужно минимум 2 периода");

        var sql = string.Join(" UNION ALL ", selects) + $" LIMIT {_maxRows}";
        var human = $"Сравнение метрики «{metric.DisplayName}» между периодами: {string.Join(" vs ", humanPeriods)}.";
        var tech = $"Шаблон: compare_periods | метрика={metric.Key} | periods={string.Join(",", intent.Periods!)}";
        return new BuiltSql(sql, pars, human, tech);
    }

    private static void ApplyFilters(Dictionary<string, string>? filters, List<string> where, Dictionary<string, object?> pars)
    {
        if (filters == null) return;
        var idx = 0;
        foreach (var kv in filters)
        {
            var col = kv.Key.ToLowerInvariant() switch
            {
                "city" or "city_id" => "city_id",
                "status" or "status_order" => "status_order",
                _ => null
            };
            if (col == null) continue;
            var p = $"$flt{idx++}";
            where.Add($"{col} = {p}");
            pars[p] = kv.Value;
        }
    }

    private static void ApplyFiltersHuman(Dictionary<string, string>? filters, List<string> parts)
    {
        if (filters == null) return;
        foreach (var kv in filters)
            parts.Add($"фильтр {kv.Key} = {kv.Value}");
    }

    private static string FormatFilters(Dictionary<string, string>? filters) =>
        filters == null || filters.Count == 0 ? "-" : string.Join(",", filters.Select(kv => $"{kv.Key}={kv.Value}"));
}
