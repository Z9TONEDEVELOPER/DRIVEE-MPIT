using System.Diagnostics;
using DriveeDataSpace.Core.Models;
using Microsoft.Extensions.Configuration;

namespace DriveeDataSpace.Core.Services;

public class QueryExecutor
{
    private readonly int _timeoutSeconds;
    private readonly int _maxRows;
    private readonly SqlGuard _sqlGuard;
    private readonly DataSourceService _dataSources;

    public QueryExecutor(IConfiguration configuration, SqlGuard sqlGuard, DataSourceService dataSources)
    {
        _timeoutSeconds = int.TryParse(configuration["Data:CommandTimeoutSeconds"], out var timeoutSeconds) ? timeoutSeconds : 15;
        _maxRows = int.TryParse(configuration["Data:MaxRows"], out var maxRows) ? maxRows : 10000;
        _sqlGuard = sqlGuard;
        _dataSources = dataSources;
    }

    public QueryResult Execute(BuiltSql builtSql, ValidatedIntent intent)
    {
        var guardReport = _sqlGuard.Validate(builtSql.Sql, builtSql.Parameters, intent);
        if (!guardReport.Ok)
            throw new InvalidOperationException($"Guardrails: {guardReport.Reason}");

        var activeDataSource = _dataSources.GetActive();
        using var connection = _dataSources.OpenReadOnlyConnection(activeDataSource);
        using var command = connection.CreateCommand();
        command.CommandText = builtSql.Sql;
        command.CommandTimeout = _timeoutSeconds;

        foreach (var parameter in builtSql.Parameters)
        {
            var dbParameter = command.CreateParameter();
            dbParameter.ParameterName = parameter.Key;
            dbParameter.Value = parameter.Value ?? DBNull.Value;
            command.Parameters.Add(dbParameter);
        }

        var stopwatch = Stopwatch.StartNew();
        using var reader = command.ExecuteReader();
        var result = new QueryResult();

        for (var index = 0; index < reader.FieldCount; index++)
            result.Columns.Add(reader.GetName(index));

        while (reader.Read())
        {
            if (result.Rows.Count >= _maxRows)
                break;

            var row = new List<object?>(reader.FieldCount);
            for (var index = 0; index < reader.FieldCount; index++)
                row.Add(reader.IsDBNull(index) ? null : reader.GetValue(index));

            result.Rows.Add(row);
        }

        stopwatch.Stop();
        result.DurationMs = stopwatch.ElapsedMilliseconds;
        return result;
    }
}
