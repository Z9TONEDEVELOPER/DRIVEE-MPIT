using System.Text;
using NexusDataSpace.Core.Models;

namespace NexusDataSpace.Core.Services;

public sealed record BuiltSql(
    string Sql,
    Dictionary<string, object?> Parameters,
    string Signature);

public class SqlBuilder
{
    private readonly DataSourceService? _dataSources;

    public SqlBuilder()
    {
    }

    public SqlBuilder(DataSourceService dataSources)
    {
        _dataSources = dataSources;
    }

    public BuiltSql Build(ValidatedIntent intent)
    {
        var dialect = SqlBuildDialect.For(_dataSources?.GetActive().Provider);
        return intent.ComparisonRanges.Count > 0
            ? BuildComparisonQuery(intent, dialect)
            : BuildMetricQuery(intent, dialect);
    }

    private static BuiltSql BuildMetricQuery(ValidatedIntent intent, SqlBuildDialect dialect)
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

        sqlBuilder.Append($"{BuildMetricExpression(intent.Metric, dialect)} AS \"{intent.Metric.Key}\" ");
        sqlBuilder.Append($"FROM {intent.Source.Table} ");

        if (intent.DateRange != null)
        {
            var fromParameter = dialect.ParameterName(parameterIndex++);
            var toParameter = dialect.ParameterName(parameterIndex++);
            whereClauses.Add($"{intent.DateRange.DateColumn} >= {fromParameter} AND {intent.DateRange.DateColumn} < {toParameter}");
            parameters[fromParameter] = dialect.DateParameter(intent.DateRange.FromUtc);
            parameters[toParameter] = dialect.DateParameter(intent.DateRange.ToExclusiveUtc);
        }

        AppendFilterClauses(intent.Filters, whereClauses, parameters, ref parameterIndex, dialect);

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

    private static BuiltSql BuildComparisonQuery(ValidatedIntent intent, SqlBuildDialect dialect)
    {
        var innerSelects = new List<string>();
        var parameters = new Dictionary<string, object?>();
        var parameterIndex = 0;

        for (var index = 0; index < intent.ComparisonRanges.Count; index++)
        {
            var range = intent.ComparisonRanges[index];
            var whereClauses = new List<string>();
            var labelParameter = dialect.ParameterName(parameterIndex++);
            var sortParameter = dialect.ParameterName(parameterIndex++);
            var fromParameter = dialect.ParameterName(parameterIndex++);
            var toParameter = dialect.ParameterName(parameterIndex++);

            parameters[labelParameter] = range.Label;
            parameters[sortParameter] = index;
            parameters[fromParameter] = dialect.DateParameter(range.FromUtc);
            parameters[toParameter] = dialect.DateParameter(range.ToExclusiveUtc);

            whereClauses.Add($"{range.DateColumn} >= {fromParameter} AND {range.DateColumn} < {toParameter}");
            AppendFilterClauses(intent.Filters, whereClauses, parameters, ref parameterIndex, dialect);

            innerSelects.Add(
                $"SELECT {sortParameter} AS __sort, {labelParameter} AS period, {BuildMetricExpression(intent.Metric, dialect)} AS \"{intent.Metric.Key}\" " +
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

    private static string BuildMetricExpression(MetricDefinition metric, SqlBuildDialect dialect) => metric.Aggregation switch
    {
        "count" => $"COUNT({metric.Expression})",
        "sum" => dialect.Round($"SUM({metric.Expression})"),
        "avg" => dialect.Round($"AVG({metric.Expression})"),
        "max" => dialect.Round($"MAX({metric.Expression})"),
        "formula" => metric.Expression,
        _ => throw new InvalidOperationException($"Unsupported aggregation: {metric.Aggregation}")
    };

    private static void AppendFilterClauses(
        IReadOnlyList<ValidatedFilter> filters,
        ICollection<string> whereClauses,
        IDictionary<string, object?> parameters,
        ref int parameterIndex,
        SqlBuildDialect dialect)
    {
        foreach (var filter in filters)
        {
            if (filter.Operator is "in" or "not_in")
            {
                var parameterNames = new List<string>();
                foreach (var value in filter.Values)
                {
                    var parameterName = dialect.ParameterName(parameterIndex++);
                    parameterNames.Add(parameterName);
                    parameters[parameterName] = value;
                }

                whereClauses.Add($"{filter.Definition.Column} {(filter.Operator == "in" ? "IN" : "NOT IN")} ({string.Join(", ", parameterNames)})");
                continue;
            }

            var singleParameter = dialect.ParameterName(parameterIndex++);
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

internal sealed record SqlBuildDialect(string Provider)
{
    public static SqlBuildDialect For(string? provider) =>
        new(string.IsNullOrWhiteSpace(provider) ? DataSourceProviders.Sqlite : provider.Trim().ToLowerInvariant());

    public string ParameterName(int index) =>
        string.Equals(Provider, DataSourceProviders.PostgreSql, StringComparison.OrdinalIgnoreCase)
            ? $"@p{index}"
            : $"$p{index}";

    public object DateParameter(DateTime value) =>
        string.Equals(Provider, DataSourceProviders.PostgreSql, StringComparison.OrdinalIgnoreCase)
            ? DateTime.SpecifyKind(value, DateTimeKind.Unspecified)
            : value.ToString("yyyy-MM-dd HH:mm:ss");

    public string Round(string expression) =>
        string.Equals(Provider, DataSourceProviders.PostgreSql, StringComparison.OrdinalIgnoreCase)
            ? $"ROUND(({expression})::numeric, 2)"
            : $"ROUND({expression}, 2)";
}
