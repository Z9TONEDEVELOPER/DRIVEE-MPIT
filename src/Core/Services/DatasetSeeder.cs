using System.Diagnostics;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace NexusDataSpace.Core.Services;

public class DatasetSeeder
{
    private readonly string _dbPath;
    private readonly string _csvPath;
    private readonly ILogger<DatasetSeeder> _log;

    public DatasetSeeder(IConfiguration config, IHostEnvironment environment, ILogger<DatasetSeeder> log)
    {
        _dbPath = DataPathResolver.Resolve(environment, config["Data:AnalyticsDb"], "Data/nexus-data-space.db");
        _csvPath = DataPathResolver.Resolve(environment, config["Data:DatasetCsvPath"], "Data/train.csv");
        _log = log;
    }

    public void EnsureSeeded()
    {
        var dir = Path.GetDirectoryName(_dbPath);
        if (!string.IsNullOrEmpty(dir)) Directory.CreateDirectory(dir);

        try
        {
            EnsureSeededCore();
        }
        catch (SqliteException ex) when (IsMalformedDatabase(ex))
        {
            _log.LogWarning(ex, "Analytics DB at {Path} is malformed. Rebuilding from source dataset.", _dbPath);
            try
            {
                BackupDatabaseArtifacts();
            }
            catch (Exception backupEx) when (backupEx is UnauthorizedAccessException or IOException)
            {
                throw new InvalidOperationException(
                    $"Analytics DB at '{_dbPath}' is corrupted and locked by another process. Stop any running Web app or Rider debug session that uses '{_dbPath}', '{_dbPath}-wal', or '{_dbPath}-shm', then start the app again.",
                    backupEx);
            }
            EnsureSeededCore();
        }
    }

    private void EnsureSeededCore()
    {
        using var conn = new SqliteConnection(BuildConnectionString());
        conn.Open();

        if (HasOrdersTable(conn))
        {
            _log.LogInformation("Analytics DB already contains orders table, skipping import");
            return;
        }

        DropLegacyTables(conn);
        CreateSchema(conn);

        if (File.Exists(_csvPath))
        {
            _log.LogInformation("Importing dataset from {Path}", _csvPath);
            ImportCsv(conn, _csvPath);
            BuildOrdersTable(conn);
        }
        else
        {
            _log.LogWarning("CSV not found at {Path}. Generating synthetic sample.", _csvPath);
            SyntheticSeed(conn);
            BuildOrdersTable(conn);
        }

        CreateIndexes(conn);
        _log.LogInformation("Dataset ready");
    }

    private static bool IsMalformedDatabase(SqliteException ex) => ex.SqliteErrorCode == 11;

    private void BackupDatabaseArtifacts()
    {
        foreach (var path in GetDatabaseArtifacts())
        {
            if (!File.Exists(path)) continue;

            var backupPath = $"{path}.corrupt-{DateTime.UtcNow:yyyyMMddHHmmssfff}.bak";
            File.Move(path, backupPath, overwrite: true);
            _log.LogWarning("Moved corrupted SQLite artifact from {Path} to {BackupPath}", path, backupPath);
        }
    }

    private IEnumerable<string> GetDatabaseArtifacts()
    {
        yield return _dbPath;
        yield return $"{_dbPath}-wal";
        yield return $"{_dbPath}-shm";
    }

    private string BuildConnectionString() => new SqliteConnectionStringBuilder
    {
        DataSource = _dbPath,
        Pooling = false
    }.ToString();

    private static bool HasOrdersTable(SqliteConnection c)
    {
        using var cmd = c.CreateCommand();
        cmd.CommandText = "SELECT name FROM sqlite_master WHERE type='table' AND name='orders'";
        var r = cmd.ExecuteScalar();
        if (r == null) return false;
        using var cmd2 = c.CreateCommand();
        cmd2.CommandText = "SELECT COUNT(*) FROM orders";
        return Convert.ToInt64(cmd2.ExecuteScalar()) > 0;
    }

    private static void DropLegacyTables(SqliteConnection c)
    {
        using var cmd = c.CreateCommand();
        cmd.CommandText = @"
            DROP TABLE IF EXISTS rides;
            DROP TABLE IF EXISTS drivers;
            DROP TABLE IF EXISTS orders_raw;
            DROP TABLE IF EXISTS orders;";
        cmd.ExecuteNonQuery();
    }

    private static void CreateSchema(SqliteConnection c)
    {
        using var cmd = c.CreateCommand();
        cmd.CommandText = @"
            CREATE TABLE orders_raw (
                city_id                       INTEGER,
                order_id                      TEXT,
                tender_id                     TEXT,
                user_id                       TEXT,
                driver_id                     TEXT,
                offset_hours                  INTEGER,
                status_order                  TEXT,
                status_tender                 TEXT,
                order_timestamp               TEXT,
                tender_timestamp              TEXT,
                driveraccept_timestamp        TEXT,
                driverarrived_timestamp       TEXT,
                driverstarttheride_timestamp  TEXT,
                driverdone_timestamp          TEXT,
                clientcancel_timestamp        TEXT,
                drivercancel_timestamp        TEXT,
                order_modified_local          TEXT,
                cancel_before_accept_local    TEXT,
                distance_in_meters            INTEGER,
                duration_in_seconds           INTEGER,
                price_order_local             REAL,
                price_tender_local            REAL,
                price_start_local             REAL
            );";
        cmd.ExecuteNonQuery();

        using var pragma = c.CreateCommand();
        pragma.CommandText = "PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;";
        pragma.ExecuteNonQuery();
    }

    private void ImportCsv(SqliteConnection c, string path)
    {
        var sw = Stopwatch.StartNew();
        using var reader = new StreamReader(path);
        var header = reader.ReadLine();
        if (header == null) throw new InvalidOperationException("Empty CSV");

        using var tx = c.BeginTransaction();
        using var cmd = c.CreateCommand();
        cmd.Transaction = tx;
        cmd.CommandText = @"
            INSERT INTO orders_raw VALUES
            ($cid,$oid,$tid,$uid,$did,$off,$so,$st,$ot,$tt,$at,$ar,$rs,$dd,$cc,$dc,$om,$cb,$dis,$dur,$pol,$ptl,$psl);";

        var pars = new[]
        {
            "$cid","$oid","$tid","$uid","$did","$off","$so","$st","$ot","$tt",
            "$at","$ar","$rs","$dd","$cc","$dc","$om","$cb","$dis","$dur",
            "$pol","$ptl","$psl"
        };
        foreach (var p in pars) cmd.Parameters.Add(new SqliteParameter(p, DBNull.Value));

        var line = 0;
        string? row;
        while ((row = reader.ReadLine()) != null)
        {
            var parts = row.Split(',');
            if (parts.Length < 23) continue;

            for (var i = 0; i < 23; i++)
            {
                var v = parts[i];
                cmd.Parameters[i].Value = string.IsNullOrEmpty(v) ? DBNull.Value : ParseField(i, v);
            }
            cmd.ExecuteNonQuery();
            line++;

            if (line % 100000 == 0) _log.LogInformation("Imported {N} rows", line);
        }
        tx.Commit();
        _log.LogInformation("Imported {N} rows in {Ms} ms", line, sw.ElapsedMilliseconds);
    }

    private static object ParseField(int idx, string v) => idx switch
    {
        0 or 5 or 18 or 19 => long.TryParse(v, out var l) ? l : (object)DBNull.Value,
        20 or 21 or 22 => double.TryParse(v, System.Globalization.NumberStyles.Any,
                                          System.Globalization.CultureInfo.InvariantCulture, out var d)
                                          ? d : (object)DBNull.Value,
        _ => v
    };

    private void SyntheticSeed(SqliteConnection c)
    {
        var rnd = new Random(42);
        var statuses = new[] { "done", "done", "done", "done", "cancel", "delete" };
        var tenderStatuses = new[] { "done", "accept", "decline", "cancel", "wait" };
        var start = DateTime.UtcNow.Date.AddDays(-120);

        using var tx = c.BeginTransaction();
        using var cmd = c.CreateCommand();
        cmd.Transaction = tx;
        cmd.CommandText = @"INSERT INTO orders_raw
            (city_id,order_id,tender_id,user_id,driver_id,offset_hours,status_order,status_tender,
             order_timestamp,driverdone_timestamp,distance_in_meters,duration_in_seconds,
             price_order_local,price_start_local)
            VALUES($c,$oid,$tid,$u,$d,9,$so,$st,$ot,$dd,$dis,$dur,$pol,$psl)";
        var plist = new[] { "$c","$oid","$tid","$u","$d","$so","$st","$ot","$dd","$dis","$dur","$pol","$psl" };
        foreach (var p in plist) cmd.Parameters.Add(new SqliteParameter(p, DBNull.Value));

        var orderN = 1;
        for (var day = 0; day < 120; day++)
        {
            var date = start.AddDays(day);
            var ordersToday = 150 + rnd.Next(100);
            for (var k = 0; k < ordersToday; k++)
            {
                var ts = date.AddHours(rnd.Next(24)).AddMinutes(rnd.Next(60));
                var status = statuses[rnd.Next(statuses.Length)];
                var dur = 120 + rnd.Next(2400);
                var dis = 500 + rnd.Next(15000);
                var price = Math.Round(120 + dis * 0.02, 2);
                var orderId = $"ord_{orderN:X8}";
                var tenders = 1 + rnd.Next(4);

                for (var t = 0; t < tenders; t++)
                {
                    cmd.Parameters[0].Value = 67;
                    cmd.Parameters[1].Value = orderId;
                    cmd.Parameters[2].Value = $"tdr_{orderN:X8}_{t}";
                    cmd.Parameters[3].Value = $"usr_{rnd.Next(9999):X4}";
                    cmd.Parameters[4].Value = $"drv_{rnd.Next(999):X3}";
                    cmd.Parameters[5].Value = t == tenders - 1 ? status : "decline";
                    cmd.Parameters[6].Value = status == "done" ? (t == tenders - 1 ? "done" : "decline") : tenderStatuses[rnd.Next(tenderStatuses.Length)];
                    cmd.Parameters[7].Value = ts.ToString("yyyy-MM-dd HH:mm:ss");
                    cmd.Parameters[8].Value = status == "done" ? ts.AddSeconds(dur).ToString("yyyy-MM-dd HH:mm:ss") : (object)DBNull.Value;
                    cmd.Parameters[9].Value = status == "done" ? dis : (object)DBNull.Value;
                    cmd.Parameters[10].Value = status == "done" ? dur : (object)DBNull.Value;
                    cmd.Parameters[11].Value = status == "done" ? price : (object)DBNull.Value;
                    cmd.Parameters[12].Value = Math.Round(price * 0.95, 2);
                    cmd.ExecuteNonQuery();
                }
                orderN++;
            }
        }
        tx.Commit();
    }

    private static void BuildOrdersTable(SqliteConnection c)
    {
        using var cmd = c.CreateCommand();
        cmd.CommandText = @"
            CREATE TABLE orders AS
            SELECT
                order_id,
                MAX(city_id)                                          AS city_id,
                MAX(status_order)                                     AS status_order,
                MIN(order_timestamp)                                  AS order_timestamp,
                MAX(driverdone_timestamp)                             AS driverdone_timestamp,
                MAX(distance_in_meters)                               AS distance_in_meters,
                MAX(duration_in_seconds)                              AS duration_in_seconds,
                MAX(price_order_local)                                AS price_order_local,
                MAX(price_start_local)                                AS price_start_local,
                COUNT(DISTINCT tender_id)                             AS tender_count,
                SUM(CASE WHEN status_tender IN ('accept','done') THEN 1 ELSE 0 END) AS accepts
            FROM orders_raw
            GROUP BY order_id;";
        cmd.ExecuteNonQuery();
    }

    private static void CreateIndexes(SqliteConnection c)
    {
        using var cmd = c.CreateCommand();
        cmd.CommandText = @"
            CREATE INDEX IF NOT EXISTS ix_orders_ts      ON orders(order_timestamp);
            CREATE INDEX IF NOT EXISTS ix_orders_status  ON orders(status_order);
            CREATE INDEX IF NOT EXISTS ix_orders_city    ON orders(city_id);
            CREATE INDEX IF NOT EXISTS ix_raw_order_id   ON orders_raw(order_id);
            CREATE INDEX IF NOT EXISTS ix_raw_ts         ON orders_raw(order_timestamp);";
        cmd.ExecuteNonQuery();
    }
}
