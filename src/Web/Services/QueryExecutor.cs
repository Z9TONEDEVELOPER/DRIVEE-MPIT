using System.Diagnostics;
using DriveeDataSpace.Web.Models;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;

namespace DriveeDataSpace.Web.Services;

public class QueryExecutor
{
    private readonly string _dbPath;
    private readonly int _timeoutSeconds;
    private readonly int _maxRows;
    private readonly SqlGuard _sqlGuard;

    public QueryExecutor(IConfiguration configuration, IHostEnvironment environment, SqlGuard sqlGuard)
    {
        _dbPath = DataPathResolver.Resolve(environment, configuration["Data:AnalyticsDb"], "Data/drivee.db");
        _timeoutSeconds = int.TryParse(configuration["Data:CommandTimeoutSeconds"], out var timeoutSeconds) ? timeoutSeconds : 15;
        _maxRows = int.TryParse(configuration["Data:MaxRows"], out var maxRows) ? maxRows : 10000;
        _sqlGuard = sqlGuard;
    }

    public QueryResult Execute(BuiltSql builtSql, ValidatedIntent intent)
    {
        var guardReport = _sqlGuard.Validate(builtSql.Sql, builtSql.Parameters, intent);
        if (!guardReport.Ok)
            throw new InvalidOperationException($"Guardrails: {guardReport.Reason}");

        var connectionStringBuilder = new SqliteConnectionStringBuilder
        {
            DataSource = _dbPath,
            Mode = SqliteOpenMode.ReadOnly
        };

        using var connection = new SqliteConnection(connectionStringBuilder.ConnectionString);
        connection.Open();

        using var command = connection.CreateCommand();
        command.CommandText = builtSql.Sql;
        command.CommandTimeout = _timeoutSeconds;

        foreach (var parameter in builtSql.Parameters)
            command.Parameters.AddWithValue(parameter.Key, parameter.Value ?? DBNull.Value);

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
