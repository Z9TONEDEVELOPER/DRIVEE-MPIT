using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;

namespace DriveeDataSpace.Web.Services;

public sealed record AnalyticsTimeContext(
    DateTime SystemTodayUtc,
    DateTime? LatestDataTimestampUtc,
    DateTime AnchorDateUtc,
    bool UsesDataAnchor);

public class AnalyticsTimeService
{
    private readonly string _dbPath;

    public AnalyticsTimeService(IConfiguration config, IHostEnvironment environment)
    {
        _dbPath = DataPathResolver.Resolve(environment, config["Data:AnalyticsDb"], "Data/drivee.db");
    }

    public AnalyticsTimeContext GetContext()
    {
        var systemToday = DateTime.UtcNow.Date;
        var latestDataTimestamp = TryGetLatestOrderTimestampUtc();
        var anchorDate = latestDataTimestamp.HasValue && latestDataTimestamp.Value.Date < systemToday
            ? latestDataTimestamp.Value.Date
            : systemToday;

        return new AnalyticsTimeContext(
            systemToday,
            latestDataTimestamp,
            anchorDate,
            latestDataTimestamp.HasValue && latestDataTimestamp.Value.Date < systemToday);
    }

    private DateTime? TryGetLatestOrderTimestampUtc()
    {
        try
        {
            var csb = new SqliteConnectionStringBuilder
            {
                DataSource = _dbPath,
                Mode = SqliteOpenMode.ReadOnly
            };

            using var conn = new SqliteConnection(csb.ConnectionString);
            conn.Open();

            using var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT MAX(order_timestamp) FROM orders";
            var value = cmd.ExecuteScalar()?.ToString();

            return DateTime.TryParse(value, out var parsed)
                ? DateTime.SpecifyKind(parsed, DateTimeKind.Utc)
                : null;
        }
        catch
        {
            return null;
        }
    }
}
