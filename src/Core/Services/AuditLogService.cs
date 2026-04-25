using DriveeDataSpace.Core.Models;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;

namespace DriveeDataSpace.Core.Services;

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
}
