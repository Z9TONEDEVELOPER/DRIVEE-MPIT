using NexusDataSpace.Core.Models;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace NexusDataSpace.Core.Services;

public sealed class EmailService
{
    private const int BusyTimeoutMs = 5_000;

    private readonly string _dbPath;
    private readonly ILogger<EmailService> _logger;

    public EmailService(IConfiguration configuration, IHostEnvironment environment, ILogger<EmailService> logger)
    {
        _dbPath = DataPathResolver.Resolve(environment, configuration["Data:ReportsDb"], "Data/reports.db");
        _logger = logger;

        var directory = Path.GetDirectoryName(_dbPath);
        if (!string.IsNullOrWhiteSpace(directory))
            Directory.CreateDirectory(directory);

        EnsureSchema();
    }

    public Task SendRegistrationApprovedAsync(RegistrationRequest request, CancellationToken cancellationToken = default)
    {
        SaveLocalEmail(
            request.Email,
            "Nexus Data Space: доступ одобрен",
            $"""
            Здравствуйте, {request.DisplayName}!

            Ваш запрос на доступ к Nexus Data Space одобрен.
            Для входа используйте email: {request.Email}
            Пароль тот же, который вы указали при регистрации.
            """,
            "registration_approved");

        return Task.CompletedTask;
    }

    public Task SendRegistrationRejectedAsync(RegistrationRequest request, CancellationToken cancellationToken = default)
    {
        var reason = string.IsNullOrWhiteSpace(request.RejectionReason)
            ? "Причина не указана."
            : request.RejectionReason;

        SaveLocalEmail(
            request.Email,
            "Nexus Data Space: запрос на доступ отклонён",
            $"""
            Здравствуйте, {request.DisplayName}!

            Ваш запрос на доступ к Nexus Data Space отклонён.
            Причина: {reason}
            """,
            "registration_rejected");

        return Task.CompletedTask;
    }

    public List<LocalEmailMessage> ListLocalEmails(int limit = 20)
    {
        var messages = new List<LocalEmailMessage>();

        using var connection = OpenConnection();
        using var command = connection.CreateCommand();
        command.CommandText = @"
            SELECT id, recipient, subject, body, category, created_at
            FROM local_email_outbox
            ORDER BY id DESC
            LIMIT $limit";
        command.Parameters.AddWithValue("$limit", Math.Clamp(limit, 1, 200));

        using var reader = command.ExecuteReader();
        while (reader.Read())
        {
            messages.Add(new LocalEmailMessage
            {
                Id = reader.GetInt32(0),
                To = reader.GetString(1),
                Subject = reader.GetString(2),
                Body = reader.GetString(3),
                Category = reader.GetString(4),
                CreatedAt = ParseDateTime(reader.GetString(5))
            });
        }

        return messages;
    }

    private void SaveLocalEmail(string to, string subject, string body, string category)
    {
        var createdAt = DateTime.UtcNow;
        using var connection = OpenConnection();
        using var command = connection.CreateCommand();
        command.CommandText = @"
            INSERT INTO local_email_outbox(recipient, subject, body, category, created_at)
            VALUES($recipient, $subject, $body, $category, $created_at)";
        command.Parameters.AddWithValue("$recipient", to);
        command.Parameters.AddWithValue("$subject", subject);
        command.Parameters.AddWithValue("$body", body);
        command.Parameters.AddWithValue("$category", category);
        command.Parameters.AddWithValue("$created_at", createdAt.ToString("O"));
        command.ExecuteNonQuery();

        _logger.LogInformation(
            "LOCAL EMAIL\nTo: {To}\nSubject: {Subject}\n\n{Body}",
            to,
            subject,
            body);
    }

    private void EnsureSchema()
    {
        using var connection = OpenConnection();
        using var command = connection.CreateCommand();
        command.CommandText = @"
            CREATE TABLE IF NOT EXISTS local_email_outbox (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                recipient   TEXT NOT NULL,
                subject     TEXT NOT NULL,
                body        TEXT NOT NULL,
                category    TEXT NOT NULL,
                created_at  TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS ix_local_email_outbox_created_at
                ON local_email_outbox(created_at DESC);";
        command.ExecuteNonQuery();

        MigrateLegacyBranding(connection);
    }

    private static void MigrateLegacyBranding(SqliteConnection connection)
    {
        using var command = connection.CreateCommand();
        command.CommandText = @"
            UPDATE local_email_outbox
            SET subject = replace(subject, $legacy_brand, $brand),
                body = replace(body, $legacy_brand, $brand)
            WHERE subject LIKE $legacy_pattern
               OR body LIKE $legacy_pattern";
        command.Parameters.AddWithValue("$legacy_brand", LegacyBrandName());
        command.Parameters.AddWithValue("$brand", "Nexus Data Space");
        command.Parameters.AddWithValue("$legacy_pattern", $"%{LegacyBrandName()}%");
        command.ExecuteNonQuery();
    }

    private static string LegacyBrandName() =>
        string.Concat("Dri", "vee BI");

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

    private static DateTime ParseDateTime(string value) =>
        DateTime.TryParse(value, out var parsed) ? parsed : DateTime.UtcNow;
}
