using System.Diagnostics;
using DriveeDataSpace.Core.Models;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace DriveeDataSpace.Core.Services;

public class QueryExecutor
{
    private readonly int _timeoutSeconds;
    private readonly int _maxRows;
    private readonly SqlGuard _sqlGuard;
    private readonly DataSourceService _dataSources;
    private readonly TenantContext _tenantContext;
    private readonly LoadCacheService _cache;
    private readonly OperationalMetricsService _metrics;
    private readonly ILogger<QueryExecutor> _logger;
    private readonly SemaphoreSlim _sqlSemaphore;
    private readonly TimeSpan _sqlQueueTimeout;

    public QueryExecutor(
        IConfiguration configuration,
        SqlGuard sqlGuard,
        DataSourceService dataSources,
        TenantContext tenantContext,
        LoadCacheService cache,
        OperationalMetricsService metrics,
        ILogger<QueryExecutor> logger)
    {
        _timeoutSeconds = int.TryParse(configuration["Data:CommandTimeoutSeconds"], out var timeoutSeconds) ? timeoutSeconds : 15;
        _maxRows = int.TryParse(configuration["Data:MaxRows"], out var maxRows) ? maxRows : 10000;
        _sqlGuard = sqlGuard;
        _dataSources = dataSources;
        _tenantContext = tenantContext;
        _cache = cache;
        _metrics = metrics;
        _logger = logger;
        _sqlSemaphore = new SemaphoreSlim(ReadBoundedInt(configuration, "Load:MaxConcurrentSqlQueries", 4, 1, 128));
        _sqlQueueTimeout = TimeSpan.FromSeconds(ReadBoundedInt(configuration, "Load:SqlQueueTimeoutSeconds", 8, 1, 300));
    }

    public QueryResult Execute(BuiltSql builtSql, ValidatedIntent intent)
    {
        var guardReport = _sqlGuard.Validate(builtSql.Sql, builtSql.Parameters, intent);
        if (!guardReport.Ok)
            throw new InvalidOperationException($"Guardrails: {guardReport.Reason}");

        var activeDataSource = _dataSources.GetActive();
        var cacheKey = LoadCacheService.BuildQueryResultKey(
            _tenantContext.CompanyId,
            activeDataSource.Id,
            builtSql.Signature,
            builtSql.Parameters);
        if (_cache.TryGetQueryResult(cacheKey, out var cachedResult) && cachedResult != null)
        {
            _metrics.RecordResultCacheHit(_tenantContext.CompanyId);
            _logger.LogInformation(
                "SQL result cache hit. company_id={CompanyId}, data_source_id={DataSourceId}, rows={Rows}",
                _tenantContext.CompanyId,
                activeDataSource.Id,
                cachedResult.RowCount);
            return cachedResult;
        }

        if (!_sqlSemaphore.Wait(_sqlQueueTimeout))
        {
            _metrics.RecordSqlExecution(_tenantContext.CompanyId, false, 0);
            throw new InvalidOperationException("Too many SQL queries are running. Try again in a few seconds.");
        }

        var stopwatch = Stopwatch.StartNew();
        var success = false;
        try
        {
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
            _cache.SetQueryResult(cacheKey, result);
            success = true;
            return result;
        }
        finally
        {
            if (stopwatch.IsRunning)
                stopwatch.Stop();
            _metrics.RecordSqlExecution(_tenantContext.CompanyId, success, stopwatch.ElapsedMilliseconds);
            _sqlSemaphore.Release();
        }
    }

    private static int ReadBoundedInt(IConfiguration configuration, string key, int fallback, int min, int max) =>
        int.TryParse(configuration[key], out var value) ? Math.Clamp(value, min, max) : fallback;
}
