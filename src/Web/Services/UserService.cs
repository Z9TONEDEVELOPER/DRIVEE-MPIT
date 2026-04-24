using System.Security.Cryptography;
using System.Net.Mail;
using DriveeDataSpace.Web.Models;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;

namespace DriveeDataSpace.Web.Services;

public sealed class UserService
{
    private const int SaltSize = 16;
    private const int KeySize = 32;
    private const int Iterations = 100_000;
    private const int BusyTimeoutMs = 5_000;

    private readonly string _dbPath;
    private readonly List<SeedUserOptions> _seedUsers;

    public UserService(IConfiguration configuration, IHostEnvironment environment)
    {
        _dbPath = DataPathResolver.Resolve(environment, configuration["Data:ReportsDb"], "Data/reports.db");
        _seedUsers = configuration.GetSection("Auth:SeedUsers").Get<List<SeedUserOptions>>() ?? new List<SeedUserOptions>();

        var directory = Path.GetDirectoryName(_dbPath);
        if (!string.IsNullOrWhiteSpace(directory))
        {
            Directory.CreateDirectory(directory);
        }

        EnsureSchema();
        EnsureSeedUsers();
    }

    public AppUser? Authenticate(string username, string password)
    {
        if (string.IsNullOrWhiteSpace(username) || string.IsNullOrWhiteSpace(password))
        {
            return null;
        }

        using var connection = OpenConnection();
        using var command = connection.CreateCommand();
        command.CommandText = @"
            SELECT id, username, email, display_name, role, is_active, created_at, last_login_at, password_hash, password_salt
            FROM users
            WHERE normalized_username = $username OR normalized_email = $username";
        command.Parameters.AddWithValue("$username", NormalizeUsername(username));

        AppUser user;
        string storedHash;
        string storedSalt;
        using (var reader = command.ExecuteReader())
        {
            if (!reader.Read())
            {
                return null;
            }

            user = ReadUser(reader);
            if (!user.IsActive)
            {
                return null;
            }

            storedHash = reader.GetString(8);
            storedSalt = reader.GetString(9);
        }

        if (!VerifyPassword(password, storedSalt, storedHash))
        {
            return null;
        }

        var lastLoginAt = DateTime.UtcNow;
        TouchLastLogin(connection, user.Id, lastLoginAt);
        user.LastLoginAt = lastLoginAt;
        return user;
    }

    public AppUser? FindById(int id)
    {
        using var connection = OpenConnection();
        using var command = connection.CreateCommand();
        command.CommandText = @"
            SELECT id, username, email, display_name, role, is_active, created_at, last_login_at
            FROM users
            WHERE id = $id";
        command.Parameters.AddWithValue("$id", id);

        using var reader = command.ExecuteReader();
        return reader.Read() ? ReadUser(reader) : null;
    }

    public AppUser? FindByUsername(string username)
    {
        if (string.IsNullOrWhiteSpace(username))
        {
            return null;
        }

        using var connection = OpenConnection();
        using var command = connection.CreateCommand();
        command.CommandText = @"
            SELECT id, username, email, display_name, role, is_active, created_at, last_login_at
            FROM users
            WHERE normalized_username = $username OR normalized_email = $username";
        command.Parameters.AddWithValue("$username", NormalizeUsername(username));

        using var reader = command.ExecuteReader();
        return reader.Read() ? ReadUser(reader) : null;
    }

    public List<AppUserSummary> ListUsers()
    {
        var users = new List<AppUserSummary>();

        using var connection = OpenConnection();
        using var command = connection.CreateCommand();
        command.CommandText = @"
            SELECT
                u.id,
                u.username,
                u.email,
                u.display_name,
                u.role,
                u.is_active,
                u.created_at,
                u.last_login_at,
                COUNT(r.id) AS report_count
            FROM users u
            LEFT JOIN reports r ON lower(r.author) = u.normalized_username
            GROUP BY u.id, u.username, u.email, u.display_name, u.role, u.is_active, u.created_at, u.last_login_at
            ORDER BY CASE WHEN u.role = 'Admin' THEN 0 ELSE 1 END, u.display_name, u.username";

        using var reader = command.ExecuteReader();
        while (reader.Read())
        {
            users.Add(new AppUserSummary
            {
                Id = reader.GetInt32(0),
                Username = reader.GetString(1),
                Email = reader.IsDBNull(2) ? null : reader.GetString(2),
                DisplayName = reader.GetString(3),
                Role = reader.GetString(4),
                IsActive = reader.GetInt64(5) == 1,
                CreatedAt = ParseDateTime(reader.GetString(6)),
                LastLoginAt = reader.IsDBNull(7) ? null : ParseDateTime(reader.GetString(7)),
                ReportCount = reader.GetInt32(8)
            });
        }

        return users;
    }

    public AppUserDetail? GetUserDetail(int id)
    {
        using var connection = OpenConnection();
        using var command = connection.CreateCommand();
        command.CommandText = @"
            SELECT
                u.id,
                u.username,
                u.email,
                u.display_name,
                u.role,
                u.is_active,
                u.created_at,
                u.last_login_at,
                COUNT(r.id) AS report_count
            FROM users u
            LEFT JOIN reports r ON lower(r.author) = u.normalized_username
            WHERE u.id = $id
            GROUP BY u.id, u.username, u.email, u.display_name, u.role, u.is_active, u.created_at, u.last_login_at";
        command.Parameters.AddWithValue("$id", id);

        AppUserDetail detail;
        using (var reader = command.ExecuteReader())
        {
            if (!reader.Read())
            {
                return null;
            }

            detail = new AppUserDetail
            {
                Id = reader.GetInt32(0),
                Username = reader.GetString(1),
                Email = reader.IsDBNull(2) ? null : reader.GetString(2),
                DisplayName = reader.GetString(3),
                Role = reader.GetString(4),
                IsActive = reader.GetInt64(5) == 1,
                CreatedAt = ParseDateTime(reader.GetString(6)),
                LastLoginAt = reader.IsDBNull(7) ? null : ParseDateTime(reader.GetString(7)),
                ReportCount = reader.GetInt32(8)
            };
        }

        detail.RecentReports = GetRecentReportsForAuthor(detail.Username, connection);
        return detail;
    }

    public RegistrationRequest SubmitRegistrationRequest(RegistrationRequestInput input)
    {
        var displayName = NormalizeRequired(input.DisplayName, "Укажите имя.");
        var email = NormalizeEmail(input.Email);
        var organization = input.Organization?.Trim() ?? string.Empty;
        var comment = input.Comment?.Trim() ?? string.Empty;

        if (input.Password.Length < 8)
            throw new InvalidOperationException("Пароль должен быть не короче 8 символов.");

        var (salt, hash) = HashPassword(input.Password);
        using var connection = OpenConnection();
        using var transaction = connection.BeginTransaction();

        if (UserExists(connection, transaction, email))
            throw new InvalidOperationException("Пользователь с такой почтой уже существует.");

        if (PendingRegistrationRequestExists(connection, transaction, email))
            throw new InvalidOperationException("Заявка с такой почтой уже ожидает решения администратора.");

        var createdAt = DateTime.UtcNow;
        using var insert = connection.CreateCommand();
        insert.Transaction = transaction;
        insert.CommandText = @"
            INSERT INTO registration_requests(
                email,
                normalized_email,
                display_name,
                organization,
                comment,
                password_hash,
                password_salt,
                status,
                created_at)
            VALUES(
                $email,
                $normalized_email,
                $display_name,
                $organization,
                $comment,
                $password_hash,
                $password_salt,
                $status,
                $created_at);
            SELECT last_insert_rowid();";
        insert.Parameters.AddWithValue("$email", email);
        insert.Parameters.AddWithValue("$normalized_email", email);
        insert.Parameters.AddWithValue("$display_name", displayName);
        insert.Parameters.AddWithValue("$organization", organization);
        insert.Parameters.AddWithValue("$comment", comment);
        insert.Parameters.AddWithValue("$password_hash", hash);
        insert.Parameters.AddWithValue("$password_salt", salt);
        insert.Parameters.AddWithValue("$status", RegistrationRequestStatuses.Pending);
        insert.Parameters.AddWithValue("$created_at", createdAt.ToString("O"));
        var id = Convert.ToInt32(insert.ExecuteScalar());

        transaction.Commit();

        return new RegistrationRequest
        {
            Id = id,
            Email = email,
            DisplayName = displayName,
            Organization = organization,
            Comment = comment,
            Status = RegistrationRequestStatuses.Pending,
            CreatedAt = createdAt
        };
    }

    public List<RegistrationRequest> ListRegistrationRequests(string? status = null)
    {
        var requests = new List<RegistrationRequest>();

        using var connection = OpenConnection();
        using var command = connection.CreateCommand();
        command.CommandText = @"
            SELECT
                rr.id,
                rr.email,
                rr.display_name,
                rr.organization,
                rr.comment,
                rr.status,
                rr.rejection_reason,
                rr.created_at,
                rr.reviewed_at,
                rr.reviewed_by_user_id,
                reviewer.display_name
            FROM registration_requests rr
            LEFT JOIN users reviewer ON reviewer.id = rr.reviewed_by_user_id
            WHERE $status IS NULL OR rr.status = $status
            ORDER BY
                CASE rr.status
                    WHEN 'Pending' THEN 0
                    WHEN 'Approved' THEN 1
                    ELSE 2
                END,
                rr.created_at DESC";
        command.Parameters.AddWithValue("$status", string.IsNullOrWhiteSpace(status) ? DBNull.Value : status);

        using var reader = command.ExecuteReader();
        while (reader.Read())
            requests.Add(ReadRegistrationRequest(reader));

        return requests;
    }

    public RegistrationDecisionResult ApproveRegistrationRequest(int requestId, int adminUserId)
    {
        using var connection = OpenConnection();
        using var transaction = connection.BeginTransaction();

        var pending = GetPendingRegistrationRequestForUpdate(connection, transaction, requestId);
        if (pending == null)
            throw new InvalidOperationException("Заявка не найдена или уже обработана.");

        if (UserExists(connection, transaction, pending.NormalizedEmail))
            throw new InvalidOperationException("Пользователь с такой почтой уже существует.");

        var now = DateTime.UtcNow;
        using var insertUser = connection.CreateCommand();
        insertUser.Transaction = transaction;
        insertUser.CommandText = @"
            INSERT INTO users(
                username,
                normalized_username,
                email,
                normalized_email,
                display_name,
                role,
                password_hash,
                password_salt,
                is_active,
                created_at)
            VALUES(
                $username,
                $normalized_username,
                $email,
                $normalized_email,
                $display_name,
                $role,
                $password_hash,
                $password_salt,
                1,
                $created_at);
            SELECT last_insert_rowid();";
        insertUser.Parameters.AddWithValue("$username", pending.Email);
        insertUser.Parameters.AddWithValue("$normalized_username", pending.NormalizedEmail);
        insertUser.Parameters.AddWithValue("$email", pending.Email);
        insertUser.Parameters.AddWithValue("$normalized_email", pending.NormalizedEmail);
        insertUser.Parameters.AddWithValue("$display_name", pending.DisplayName);
        insertUser.Parameters.AddWithValue("$role", AppRoles.User);
        insertUser.Parameters.AddWithValue("$password_hash", pending.PasswordHash);
        insertUser.Parameters.AddWithValue("$password_salt", pending.PasswordSalt);
        insertUser.Parameters.AddWithValue("$created_at", now.ToString("O"));
        var userId = Convert.ToInt32(insertUser.ExecuteScalar());

        MarkRegistrationRequestReviewed(
            connection,
            transaction,
            requestId,
            RegistrationRequestStatuses.Approved,
            null,
            adminUserId,
            now);

        transaction.Commit();

        var request = pending.ToPublicRequest(
            RegistrationRequestStatuses.Approved,
            null,
            now,
            adminUserId);
        var user = new AppUser
        {
            Id = userId,
            Username = pending.Email,
            Email = pending.Email,
            DisplayName = pending.DisplayName,
            Role = AppRoles.User,
            IsActive = true,
            CreatedAt = now
        };

        return new RegistrationDecisionResult(request, user);
    }

    public RegistrationDecisionResult RejectRegistrationRequest(int requestId, int adminUserId, string? reason)
    {
        using var connection = OpenConnection();
        using var transaction = connection.BeginTransaction();

        var pending = GetPendingRegistrationRequestForUpdate(connection, transaction, requestId);
        if (pending == null)
            throw new InvalidOperationException("Заявка не найдена или уже обработана.");

        var now = DateTime.UtcNow;
        var rejectionReason = reason?.Trim();
        MarkRegistrationRequestReviewed(
            connection,
            transaction,
            requestId,
            RegistrationRequestStatuses.Rejected,
            rejectionReason,
            adminUserId,
            now);

        transaction.Commit();

        var request = pending.ToPublicRequest(
            RegistrationRequestStatuses.Rejected,
            rejectionReason,
            now,
            adminUserId);

        return new RegistrationDecisionResult(request, null);
    }

    private void EnsureSchema()
    {
        using var connection = OpenConnection();
        EnsurePragmas(connection);
        using var command = connection.CreateCommand();
        command.CommandText = @"
            CREATE TABLE IF NOT EXISTS users (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                username            TEXT NOT NULL,
                normalized_username TEXT NOT NULL UNIQUE,
                display_name        TEXT NOT NULL,
                role                TEXT NOT NULL,
                password_hash       TEXT NOT NULL,
                password_salt       TEXT NOT NULL,
                is_active           INTEGER NOT NULL DEFAULT 1,
                created_at          TEXT NOT NULL,
                last_login_at       TEXT NULL
            );

            CREATE UNIQUE INDEX IF NOT EXISTS ix_users_username
                ON users(normalized_username);

            CREATE TABLE IF NOT EXISTS registration_requests (
                id                   INTEGER PRIMARY KEY AUTOINCREMENT,
                email                TEXT NOT NULL,
                normalized_email     TEXT NOT NULL,
                display_name         TEXT NOT NULL,
                organization         TEXT NOT NULL DEFAULT '',
                comment              TEXT NOT NULL DEFAULT '',
                password_hash        TEXT NOT NULL,
                password_salt        TEXT NOT NULL,
                status               TEXT NOT NULL,
                rejection_reason     TEXT NULL,
                created_at           TEXT NOT NULL,
                reviewed_at          TEXT NULL,
                reviewed_by_user_id  INTEGER NULL,
                FOREIGN KEY(reviewed_by_user_id) REFERENCES users(id)
            );

            CREATE INDEX IF NOT EXISTS ix_registration_requests_status
                ON registration_requests(status, created_at);

            CREATE UNIQUE INDEX IF NOT EXISTS ix_registration_requests_pending_email
                ON registration_requests(normalized_email)
                WHERE status = 'Pending';";
        command.ExecuteNonQuery();

        EnsureColumn(connection, "users", "email", "TEXT NULL");
        EnsureColumn(connection, "users", "normalized_email", "TEXT NULL");

        using var emailIndex = connection.CreateCommand();
        emailIndex.CommandText = @"
            CREATE UNIQUE INDEX IF NOT EXISTS ix_users_email
                ON users(normalized_email)
                WHERE normalized_email IS NOT NULL AND normalized_email <> '';";
        emailIndex.ExecuteNonQuery();
    }

    private void EnsureSeedUsers()
    {
        foreach (var seedUser in _seedUsers.Where(static user => !string.IsNullOrWhiteSpace(user.Username)))
        {
            UpsertSeedUser(seedUser);
        }
    }

    private void UpsertSeedUser(SeedUserOptions seedUser)
    {
        var normalizedUsername = NormalizeUsername(seedUser.Username);
        var email = string.IsNullOrWhiteSpace(seedUser.Email) ? null : NormalizeEmail(seedUser.Email);
        var displayName = string.IsNullOrWhiteSpace(seedUser.DisplayName) ? seedUser.Username.Trim() : seedUser.DisplayName.Trim();
        var role = string.Equals(seedUser.Role, AppRoles.Admin, StringComparison.OrdinalIgnoreCase)
            ? AppRoles.Admin
            : AppRoles.User;

        using var connection = OpenConnection();
        using var transaction = connection.BeginTransaction();

        using var lookup = connection.CreateCommand();
        lookup.Transaction = transaction;
        lookup.CommandText = "SELECT id FROM users WHERE normalized_username = $username";
        lookup.Parameters.AddWithValue("$username", normalizedUsername);
        var existingId = lookup.ExecuteScalar() as long?;

        var (salt, hash) = HashPassword(string.IsNullOrWhiteSpace(seedUser.Password) ? "ChangeMe123!" : seedUser.Password);

        if (existingId.HasValue)
        {
            using var update = connection.CreateCommand();
            update.Transaction = transaction;
            update.CommandText = @"
                UPDATE users
                SET username = $plain_username,
                    email = $email,
                    normalized_email = $normalized_email,
                    display_name = $display_name,
                    role = $role,
                    password_hash = $password_hash,
                    password_salt = $password_salt,
                    is_active = 1
                WHERE id = $id";
            update.Parameters.AddWithValue("$id", existingId.Value);
            update.Parameters.AddWithValue("$plain_username", seedUser.Username.Trim());
            update.Parameters.AddWithValue("$email", (object?)email ?? DBNull.Value);
            update.Parameters.AddWithValue("$normalized_email", (object?)email ?? DBNull.Value);
            update.Parameters.AddWithValue("$display_name", displayName);
            update.Parameters.AddWithValue("$role", role);
            update.Parameters.AddWithValue("$password_hash", hash);
            update.Parameters.AddWithValue("$password_salt", salt);
            update.ExecuteNonQuery();
        }
        else
        {
            using var insert = connection.CreateCommand();
            insert.Transaction = transaction;
            insert.CommandText = @"
                INSERT INTO users(username, normalized_username, email, normalized_email, display_name, role, password_hash, password_salt, is_active, created_at)
                VALUES($plain_username, $normalized_username, $email, $normalized_email, $display_name, $role, $password_hash, $password_salt, 1, $created_at)";
            insert.Parameters.AddWithValue("$plain_username", seedUser.Username.Trim());
            insert.Parameters.AddWithValue("$normalized_username", normalizedUsername);
            insert.Parameters.AddWithValue("$email", (object?)email ?? DBNull.Value);
            insert.Parameters.AddWithValue("$normalized_email", (object?)email ?? DBNull.Value);
            insert.Parameters.AddWithValue("$display_name", displayName);
            insert.Parameters.AddWithValue("$role", role);
            insert.Parameters.AddWithValue("$password_hash", hash);
            insert.Parameters.AddWithValue("$password_salt", salt);
            insert.Parameters.AddWithValue("$created_at", DateTime.UtcNow.ToString("O"));
            insert.ExecuteNonQuery();
        }

        transaction.Commit();
    }

    private sealed record PendingRegistrationRequest(
        int Id,
        string Email,
        string NormalizedEmail,
        string DisplayName,
        string Organization,
        string Comment,
        string PasswordHash,
        string PasswordSalt,
        DateTime CreatedAt)
    {
        public RegistrationRequest ToPublicRequest(string status, string? rejectionReason, DateTime? reviewedAt, int? reviewedByUserId) =>
            new()
            {
                Id = Id,
                Email = Email,
                DisplayName = DisplayName,
                Organization = Organization,
                Comment = Comment,
                Status = status,
                RejectionReason = rejectionReason,
                CreatedAt = CreatedAt,
                ReviewedAt = reviewedAt,
                ReviewedByUserId = reviewedByUserId
            };
    }

    private static PendingRegistrationRequest? GetPendingRegistrationRequestForUpdate(
        SqliteConnection connection,
        SqliteTransaction transaction,
        int requestId)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = @"
            SELECT id, email, normalized_email, display_name, organization, comment, password_hash, password_salt, created_at
            FROM registration_requests
            WHERE id = $id AND status = $status";
        command.Parameters.AddWithValue("$id", requestId);
        command.Parameters.AddWithValue("$status", RegistrationRequestStatuses.Pending);

        using var reader = command.ExecuteReader();
        if (!reader.Read())
            return null;

        return new PendingRegistrationRequest(
            reader.GetInt32(0),
            reader.GetString(1),
            reader.GetString(2),
            reader.GetString(3),
            reader.GetString(4),
            reader.GetString(5),
            reader.GetString(6),
            reader.GetString(7),
            ParseDateTime(reader.GetString(8)));
    }

    private static void MarkRegistrationRequestReviewed(
        SqliteConnection connection,
        SqliteTransaction transaction,
        int requestId,
        string status,
        string? rejectionReason,
        int adminUserId,
        DateTime reviewedAt)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = @"
            UPDATE registration_requests
            SET status = $status,
                rejection_reason = $rejection_reason,
                reviewed_at = $reviewed_at,
                reviewed_by_user_id = $reviewed_by_user_id
            WHERE id = $id";
        command.Parameters.AddWithValue("$id", requestId);
        command.Parameters.AddWithValue("$status", status);
        command.Parameters.AddWithValue("$rejection_reason", (object?)rejectionReason ?? DBNull.Value);
        command.Parameters.AddWithValue("$reviewed_at", reviewedAt.ToString("O"));
        command.Parameters.AddWithValue("$reviewed_by_user_id", adminUserId);
        command.ExecuteNonQuery();
    }

    private static bool UserExists(SqliteConnection connection, SqliteTransaction transaction, string normalizedEmail)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = @"
            SELECT 1
            FROM users
            WHERE normalized_username = $email OR normalized_email = $email
            LIMIT 1";
        command.Parameters.AddWithValue("$email", normalizedEmail);
        return command.ExecuteScalar() != null;
    }

    private static bool PendingRegistrationRequestExists(SqliteConnection connection, SqliteTransaction transaction, string normalizedEmail)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = @"
            SELECT 1
            FROM registration_requests
            WHERE normalized_email = $email AND status = $status
            LIMIT 1";
        command.Parameters.AddWithValue("$email", normalizedEmail);
        command.Parameters.AddWithValue("$status", RegistrationRequestStatuses.Pending);
        return command.ExecuteScalar() != null;
    }

    private static void TouchLastLogin(SqliteConnection connection, int userId, DateTime lastLoginAt)
    {
        using var command = connection.CreateCommand();
        command.CommandText = "UPDATE users SET last_login_at = $last_login_at WHERE id = $id";
        command.Parameters.AddWithValue("$id", userId);
        command.Parameters.AddWithValue("$last_login_at", lastLoginAt.ToString("O"));
        command.ExecuteNonQuery();
    }

    private List<Report> GetRecentReportsForAuthor(string username, SqliteConnection connection)
    {
        var reports = new List<Report>();

        using var command = connection.CreateCommand();
        command.CommandText = @"
            SELECT id, name, user_query, intent_json, sql_text, visualization, author, created_at
            FROM reports
            WHERE lower(author) = $author
            ORDER BY id DESC
            LIMIT 20";
        command.Parameters.AddWithValue("$author", NormalizeUsername(username));

        using var reader = command.ExecuteReader();
        while (reader.Read())
        {
            reports.Add(new Report
            {
                Id = reader.GetInt32(0),
                Name = reader.GetString(1),
                UserQuery = reader.GetString(2),
                IntentJson = reader.GetString(3),
                Sql = reader.GetString(4),
                Visualization = reader.GetString(5),
                Author = reader.GetString(6),
                CreatedAt = ParseDateTime(reader.GetString(7))
            });
        }

        return reports;
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

    private static void EnsurePragmas(SqliteConnection connection)
    {
        using var journalCommand = connection.CreateCommand();
        journalCommand.CommandText = "PRAGMA journal_mode = WAL;";
        _ = journalCommand.ExecuteScalar();
    }

    private static void EnsureColumn(SqliteConnection connection, string tableName, string columnName, string definition)
    {
        using var tableInfo = connection.CreateCommand();
        tableInfo.CommandText = $"PRAGMA table_info({tableName});";

        using (var reader = tableInfo.ExecuteReader())
        {
            while (reader.Read())
            {
                if (string.Equals(reader.GetString(1), columnName, StringComparison.OrdinalIgnoreCase))
                    return;
            }
        }

        using var alter = connection.CreateCommand();
        alter.CommandText = $"ALTER TABLE {tableName} ADD COLUMN {columnName} {definition};";
        alter.ExecuteNonQuery();
    }

    private static AppUser ReadUser(SqliteDataReader reader) =>
        new()
        {
            Id = reader.GetInt32(0),
            Username = reader.GetString(1),
            Email = reader.IsDBNull(2) ? null : reader.GetString(2),
            DisplayName = reader.GetString(3),
            Role = reader.GetString(4),
            IsActive = reader.GetInt64(5) == 1,
            CreatedAt = ParseDateTime(reader.GetString(6)),
            LastLoginAt = reader.IsDBNull(7) ? null : ParseDateTime(reader.GetString(7))
        };

    private static RegistrationRequest ReadRegistrationRequest(SqliteDataReader reader) =>
        new()
        {
            Id = reader.GetInt32(0),
            Email = reader.GetString(1),
            DisplayName = reader.GetString(2),
            Organization = reader.GetString(3),
            Comment = reader.GetString(4),
            Status = reader.GetString(5),
            RejectionReason = reader.IsDBNull(6) ? null : reader.GetString(6),
            CreatedAt = ParseDateTime(reader.GetString(7)),
            ReviewedAt = reader.IsDBNull(8) ? null : ParseDateTime(reader.GetString(8)),
            ReviewedByUserId = reader.IsDBNull(9) ? null : reader.GetInt32(9),
            ReviewedByDisplayName = reader.IsDBNull(10) ? null : reader.GetString(10)
        };

    private static string NormalizeUsername(string username) =>
        username.Trim().ToLowerInvariant();

    private static string NormalizeEmail(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
            throw new InvalidOperationException("Укажите email.");

        try
        {
            var address = new MailAddress(value.Trim());
            return address.Address.Trim().ToLowerInvariant();
        }
        catch (FormatException)
        {
            throw new InvalidOperationException("Укажите корректный email.");
        }
    }

    private static string NormalizeRequired(string value, string errorMessage)
    {
        if (string.IsNullOrWhiteSpace(value))
            throw new InvalidOperationException(errorMessage);

        return value.Trim();
    }

    private static (string Salt, string Hash) HashPassword(string password)
    {
        var salt = RandomNumberGenerator.GetBytes(SaltSize);
        var hash = Rfc2898DeriveBytes.Pbkdf2(password, salt, Iterations, HashAlgorithmName.SHA256, KeySize);
        return (Convert.ToBase64String(salt), Convert.ToBase64String(hash));
    }

    private static bool VerifyPassword(string password, string encodedSalt, string encodedHash)
    {
        var salt = Convert.FromBase64String(encodedSalt);
        var expectedHash = Convert.FromBase64String(encodedHash);
        var actualHash = Rfc2898DeriveBytes.Pbkdf2(password, salt, Iterations, HashAlgorithmName.SHA256, KeySize);
        return CryptographicOperations.FixedTimeEquals(actualHash, expectedHash);
    }

    private static DateTime ParseDateTime(string value) =>
        DateTime.TryParse(value, out var parsed) ? parsed : DateTime.UtcNow;
}
