using DriveeDataSpace.Core.Models;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using System.Text;

namespace DriveeDataSpace.Core.Services;

public sealed record AuditLogEntry(
    long Id,
    int CompanyId,
    int? UserId,
    string? Username,
    string Action,
    string TargetType,
    string? TargetId,
    bool Success,
    string? Details,
    DateTimeOffset CreatedAt);

public sealed record AuditLogSummary(
    int EventsLastHour,
    int EventsLastDay,
    int FailedLastDay,
    int ActiveUsersLastDay,
    DateTimeOffset? LastEventAt);

public sealed class AuditLogService
{
    private const int BusyTimeoutMs = 5_000;
    private readonly string _dbPath;

    public AuditLogService(IConfiguration configuration, IHostEnvironment environment)
    {
        _dbPath = DataPathResolver.Resolve(environment, configuration["Data:ReportsDb"], "Data/reports.db");
        var directory = Path.GetDirectoryName(_dbPath);
        if (!string.IsNullOrWhiteSpace(directory))
            Directory.CreateDirectory(directory);

        EnsureSchema();
    }

    public void Record(
        int companyId,
        int? userId,
        string? username,
        string action,
        string targetType,
        string? targetId = null,
        bool success = true,
        string? details = null)
    {
        using var connection = OpenConnection();
        using var command = connection.CreateCommand();
        command.CommandText = @"
            INSERT INTO audit_log(company_id, user_id, username, action, target_type, target_id, success, details, created_at)
            VALUES($company_id, $user_id, $username, $action, $target_type, $target_id, $success, $details, $created_at);";
        command.Parameters.AddWithValue("$company_id", companyId <= 0 ? CompanyDefaults.DefaultCompanyId : companyId);
        command.Parameters.AddWithValue("$user_id", (object?)userId ?? DBNull.Value);
        command.Parameters.AddWithValue("$username", (object?)username ?? DBNull.Value);
        command.Parameters.AddWithValue("$action", action);
        command.Parameters.AddWithValue("$target_type", targetType);
        command.Parameters.AddWithValue("$target_id", (object?)targetId ?? DBNull.Value);
        command.Parameters.AddWithValue("$success", success ? 1 : 0);
        command.Parameters.AddWithValue("$details", (object?)details ?? DBNull.Value);
        command.Parameters.AddWithValue("$created_at", DateTime.UtcNow.ToString("O"));
        command.ExecuteNonQuery();
    }

    public IReadOnlyList<AuditLogEntry> List(
        int companyId,
        int limit = 200,
        string? action = null,
        bool errorsOnly = false)
    {
        limit = Math.Clamp(limit, 1, 1_000);
        using var connection = OpenConnection();
        using var command = connection.CreateCommand();
        var where = new List<string> { "company_id = $company_id" };
        command.Parameters.AddWithValue("$company_id", companyId <= 0 ? CompanyDefaults.DefaultCompanyId : companyId);

        if (!string.IsNullOrWhiteSpace(action))
        {
            where.Add("action = $action");
            command.Parameters.AddWithValue("$action", action.Trim());
        }

        if (errorsOnly)
            where.Add("success = 0");

        command.CommandText = $@"
            SELECT id, company_id, user_id, username, action, target_type, target_id, success, details, created_at
            FROM audit_log
            WHERE {string.Join(" AND ", where)}
            ORDER BY created_at DESC
            LIMIT $limit;";
        command.Parameters.AddWithValue("$limit", limit);

        using var reader = command.ExecuteReader();
        var entries = new List<AuditLogEntry>();
        while (reader.Read())
            entries.Add(ReadEntry(reader));

        return entries;
    }

    public IReadOnlyList<string> ListActions(int companyId, int limit = 100)
    {
        limit = Math.Clamp(limit, 1, 500);
        using var connection = OpenConnection();
        using var command = connection.CreateCommand();
        command.CommandText = @"
            SELECT action
            FROM audit_log
            WHERE company_id = $company_id
            GROUP BY action
            ORDER BY MAX(created_at) DESC
            LIMIT $limit;";
        command.Parameters.AddWithValue("$company_id", companyId <= 0 ? CompanyDefaults.DefaultCompanyId : companyId);
        command.Parameters.AddWithValue("$limit", limit);

        using var reader = command.ExecuteReader();
        var actions = new List<string>();
        while (reader.Read())
            actions.Add(reader.GetString(0));

        return actions;
    }

    public AuditLogSummary GetSummary(int companyId)
    {
        var normalizedCompanyId = companyId <= 0 ? CompanyDefaults.DefaultCompanyId : companyId;
        var hourStart = DateTime.UtcNow.AddHours(-1).ToString("O");
        var dayStart = DateTime.UtcNow.AddDays(-1).ToString("O");

        using var connection = OpenConnection();
        var eventsLastHour = ExecuteScalarInt(connection, @"
            SELECT COUNT(*)
            FROM audit_log
            WHERE company_id = $company_id AND created_at >= $from;",
            normalizedCompanyId,
            hourStart);
        var eventsLastDay = ExecuteScalarInt(connection, @"
            SELECT COUNT(*)
            FROM audit_log
            WHERE company_id = $company_id AND created_at >= $from;",
            normalizedCompanyId,
            dayStart);
        var failedLastDay = ExecuteScalarInt(connection, @"
            SELECT COUNT(*)
            FROM audit_log
            WHERE company_id = $company_id AND created_at >= $from AND success = 0;",
            normalizedCompanyId,
            dayStart);
        var activeUsersLastDay = ExecuteScalarInt(connection, @"
            SELECT COUNT(DISTINCT COALESCE(username, CAST(user_id AS TEXT)))
            FROM audit_log
            WHERE company_id = $company_id AND created_at >= $from AND (username IS NOT NULL OR user_id IS NOT NULL);",
            normalizedCompanyId,
            dayStart);
        var lastEventAt = ExecuteScalarString(connection, @"
            SELECT created_at
            FROM audit_log
            WHERE company_id = $company_id
            ORDER BY created_at DESC
            LIMIT 1;",
            normalizedCompanyId,
            null);

        return new AuditLogSummary(
            eventsLastHour,
            eventsLastDay,
            failedLastDay,
            activeUsersLastDay,
            ParseDate(lastEventAt));
    }

    private void EnsureSchema()
    {
        using var connection = OpenConnection();
        using var command = connection.CreateCommand();
        command.CommandText = @"
            CREATE TABLE IF NOT EXISTS audit_log (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                company_id  INTEGER NOT NULL DEFAULT 1,
                user_id     INTEGER NULL,
                username    TEXT NULL,
                action      TEXT NOT NULL,
                target_type TEXT NOT NULL,
                target_id   TEXT NULL,
                success     INTEGER NOT NULL,
                details     TEXT NULL,
                created_at  TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS ix_audit_log_company_created
                ON audit_log(company_id, created_at DESC);";
        command.ExecuteNonQuery();
    }

    private SqliteConnection OpenConnection()
    {
        var connectionString = new SqliteConnectionStringBuilder
        {
            DataSource = _dbPath,
            Mode = SqliteOpenMode.ReadWriteCreate,
            Pooling = true
        }.ToString();

        var connection = new SqliteConnection(connectionString);
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = $"PRAGMA busy_timeout = {BusyTimeoutMs};";
        command.ExecuteNonQuery();
        return connection;
    }

    private static AuditLogEntry ReadEntry(SqliteDataReader reader) => new(
        reader.GetInt64(0),
        reader.GetInt32(1),
        reader.IsDBNull(2) ? null : reader.GetInt32(2),
        reader.IsDBNull(3) ? null : reader.GetString(3),
        reader.GetString(4),
        reader.GetString(5),
        reader.IsDBNull(6) ? null : reader.GetString(6),
        reader.GetInt32(7) == 1,
        reader.IsDBNull(8) ? null : reader.GetString(8),
        ParseDate(reader.GetString(9)) ?? DateTimeOffset.MinValue);

    private static int ExecuteScalarInt(SqliteConnection connection, string sql, int companyId, string? from)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.Parameters.AddWithValue("$company_id", companyId);
        if (from != null)
            command.Parameters.AddWithValue("$from", from);

        var value = command.ExecuteScalar();
        return Convert.ToInt32(value ?? 0);
    }

    private static string? ExecuteScalarString(SqliteConnection connection, string sql, int companyId, string? from)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.Parameters.AddWithValue("$company_id", companyId);
        if (from != null)
            command.Parameters.AddWithValue("$from", from);

        return command.ExecuteScalar()?.ToString();
    }

    private static DateTimeOffset? ParseDate(string? value) =>
        DateTimeOffset.TryParse(value, out var parsed) ? parsed : null;
}
