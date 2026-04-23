using System.Diagnostics;
using DriveeDataSpace.Web.Models;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;

namespace DriveeDataSpace.Web.Services;

public class QueryExecutor
{
    private readonly string _dbPath;
    private readonly int _timeoutSec;
    private readonly int _maxRows;

    public QueryExecutor(IConfiguration config, IHostEnvironment environment)
    {
        _dbPath = DataPathResolver.Resolve(environment, config["Data:AnalyticsDb"], "Data/drivee.db");
        _timeoutSec = int.TryParse(config["Data:CommandTimeoutSeconds"], out var t) ? t : 15;
        _maxRows = int.TryParse(config["Data:MaxRows"], out var m) ? m : 10000;
    }

    public QueryResult Execute(string sql, IDictionary<string, object?>? parameters = null)
    {
        var (ok, reason) = SqlGuard.Validate(sql);
        if (!ok) throw new InvalidOperationException($"Guardrails: {reason}");

        var csb = new SqliteConnectionStringBuilder
        {
            DataSource = _dbPath,
            Mode = SqliteOpenMode.ReadOnly
        };

        using var conn = new SqliteConnection(csb.ConnectionString);
        conn.Open();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        cmd.CommandTimeout = _timeoutSec;

        if (parameters != null)
        {
            foreach (var kv in parameters)
                cmd.Parameters.AddWithValue(kv.Key, kv.Value ?? DBNull.Value);
        }

        var sw = Stopwatch.StartNew();
        using var reader = cmd.ExecuteReader();
        var result = new QueryResult();
        for (var i = 0; i < reader.FieldCount; i++)
            result.Columns.Add(reader.GetName(i));

        while (reader.Read())
        {
            if (result.Rows.Count >= _maxRows) break;
            var row = new List<object?>(reader.FieldCount);
            for (var i = 0; i < reader.FieldCount; i++)
                row.Add(reader.IsDBNull(i) ? null : reader.GetValue(i));
            result.Rows.Add(row);
        }
        sw.Stop();
        result.DurationMs = sw.ElapsedMilliseconds;
        return result;
    }
}
