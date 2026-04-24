using System.Text;
using DriveeDataSpace.Web.Models;

namespace DriveeDataSpace.Web.Services;

public sealed record BuiltSql(
    string Sql,
    Dictionary<string, object?> Parameters,
    string Signature);

public class SqlBuilder
{
    public BuiltSql Build(ValidatedIntent intent)
    {
        return intent.ComparisonRanges.Count > 0
            ? BuildComparisonQuery(intent)
            : BuildMetricQuery(intent);
    }

    private static BuiltSql BuildMetricQuery(ValidatedIntent intent)
    {
        var sqlBuilder = new StringBuilder();
        var parameters = new Dictionary<string, object?>();
        var whereClauses = new List<string>();
        var parameterIndex = 0;

        sqlBuilder.Append("SELECT ");

        if (intent.Dimensions.Count > 0)
        {
            sqlBuilder.Append(string.Join(
                ", ",
                intent.Dimensions.Select(dimension => $"{dimension.SqlExpression} AS \"{dimension.Alias}\"")));
            sqlBuilder.Append(", ");
        }

        sqlBuilder.Append($"{BuildMetricExpression(intent.Metric)} AS \"{intent.Metric.Key}\" ");
        sqlBuilder.Append($"FROM {intent.Source.Table} ");

        if (intent.DateRange != null)
        {
            var fromParameter = $"$p{parameterIndex++}";
            var toParameter = $"$p{parameterIndex++}";
            whereClauses.Add($"{intent.DateRange.DateColumn} >= {fromParameter} AND {intent.DateRange.DateColumn} < {toParameter}");
            parameters[fromParameter] = intent.DateRange.FromUtc.ToString("yyyy-MM-dd HH:mm:ss");
            parameters[toParameter] = intent.DateRange.ToExclusiveUtc.ToString("yyyy-MM-dd HH:mm:ss");
        }

        AppendFilterClauses(intent.Filters, whereClauses, parameters, ref parameterIndex);

        if (whereClauses.Count > 0)
            sqlBuilder.Append("WHERE ").Append(string.Join(" AND ", whereClauses)).Append(' ');

        if (intent.Dimensions.Count > 0)
        {
            sqlBuilder.Append("GROUP BY ");
            sqlBuilder.Append(string.Join(", ", intent.Dimensions.Select(dimension => dimension.SqlExpression)));
            sqlBuilder.Append(' ');
        }

        AppendOrderBy(sqlBuilder, intent.Sort);
        sqlBuilder.Append($"LIMIT {intent.Limit}");

        var sql = sqlBuilder.ToString();
        return new BuiltSql(sql, parameters, BuildSignature(sql, parameters));
    }

    private static BuiltSql BuildComparisonQuery(ValidatedIntent intent)
    {
        var innerSelects = new List<string>();
        var parameters = new Dictionary<string, object?>();
        var parameterIndex = 0;

        for (var index = 0; index < intent.ComparisonRanges.Count; index++)
        {
            var range = intent.ComparisonRanges[index];
            var whereClauses = new List<string>();
            var labelParameter = $"$p{parameterIndex++}";
            var sortParameter = $"$p{parameterIndex++}";
            var fromParameter = $"$p{parameterIndex++}";
            var toParameter = $"$p{parameterIndex++}";

            parameters[labelParameter] = range.Label;
            parameters[sortParameter] = index;
            parameters[fromParameter] = range.FromUtc.ToString("yyyy-MM-dd HH:mm:ss");
            parameters[toParameter] = range.ToExclusiveUtc.ToString("yyyy-MM-dd HH:mm:ss");

            whereClauses.Add($"{range.DateColumn} >= {fromParameter} AND {range.DateColumn} < {toParameter}");
            AppendFilterClauses(intent.Filters, whereClauses, parameters, ref parameterIndex);

            innerSelects.Add(
                $"SELECT {sortParameter} AS __sort, {labelParameter} AS period, {BuildMetricExpression(intent.Metric)} AS \"{intent.Metric.Key}\" " +
                $"FROM {intent.Source.Table} WHERE {string.Join(" AND ", whereClauses)}");
        }

        var sqlBuilder = new StringBuilder();
        sqlBuilder.Append("SELECT period, \"").Append(intent.Metric.Key).Append("\" FROM (");
        sqlBuilder.Append(string.Join(" UNION ALL ", innerSelects));
        sqlBuilder.Append(") ");

        if (intent.Sort.Count > 0)
        {
            sqlBuilder.Append("ORDER BY ");
            sqlBuilder.Append(string.Join(", ", intent.Sort.Select(sort =>
            {
                var alias = sort.Alias == intent.Metric.Key || sort.Alias == "period" ? sort.Alias : "__sort";
                return $"\"{alias}\" {sort.Direction.ToUpperInvariant()}";
            })));
            sqlBuilder.Append(' ');
        }
        else
        {
            sqlBuilder.Append("ORDER BY __sort ASC ");
        }

        sqlBuilder.Append($"LIMIT {intent.Limit}");

        var sql = sqlBuilder.ToString();
        return new BuiltSql(sql, parameters, BuildSignature(sql, parameters));
    }

    private static string BuildMetricExpression(MetricDefinition metric) => metric.Aggregation switch
    {
        "count" => $"COUNT({metric.Expression})",
        "sum" => $"ROUND(SUM({metric.Expression}), 2)",
        "avg" => $"ROUND(AVG({metric.Expression}), 2)",
        "formula" => metric.Expression,
        _ => throw new InvalidOperationException($"Unsupported aggregation: {metric.Aggregation}")
    };

    private static void AppendFilterClauses(
        IReadOnlyList<ValidatedFilter> filters,
        ICollection<string> whereClauses,
        IDictionary<string, object?> parameters,
        ref int parameterIndex)
    {
        foreach (var filter in filters)
        {
            if (filter.Operator is "in" or "not_in")
            {
                var parameterNames = new List<string>();
                foreach (var value in filter.Values)
                {
                    var parameterName = $"$p{parameterIndex++}";
                    parameterNames.Add(parameterName);
                    parameters[parameterName] = value;
                }

                whereClauses.Add($"{filter.Definition.Column} {(filter.Operator == "in" ? "IN" : "NOT IN")} ({string.Join(", ", parameterNames)})");
                continue;
            }

            var singleParameter = $"$p{parameterIndex++}";
            parameters[singleParameter] = filter.Values[0];
            whereClauses.Add($"{filter.Definition.Column} {ToSqlOperator(filter.Operator)} {singleParameter}");
        }
    }

    private static void AppendOrderBy(StringBuilder sqlBuilder, IReadOnlyList<ValidatedSort> sort)
    {
        if (sort.Count == 0)
            return;

        sqlBuilder.Append("ORDER BY ");
        sqlBuilder.Append(string.Join(
            ", ",
            sort.Select(item => $"\"{item.Alias}\" {item.Direction.ToUpperInvariant()}")));
        sqlBuilder.Append(' ');
    }

    private static string ToSqlOperator(string operatorKey) => operatorKey switch
    {
        "=" => "=",
        "!=" => "!=",
        ">" => ">",
        ">=" => ">=",
        "<" => "<",
        "<=" => "<=",
        _ => throw new InvalidOperationException($"Unsupported operator: {operatorKey}")
    };

    private static string BuildSignature(string sql, IReadOnlyDictionary<string, object?> parameters)
    {
        var normalizedSql = string.Join(" ", sql.Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries));
        var normalizedParameters = string.Join(
            ";",
            parameters
                .OrderBy(item => item.Key, StringComparer.Ordinal)
                .Select(item => $"{item.Key}={IntentValueHelper.ToDisplayString(item.Value)}"));

        return $"{normalizedSql}|{normalizedParameters}";
    }
}
