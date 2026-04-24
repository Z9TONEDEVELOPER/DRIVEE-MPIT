using System.Security.Cryptography;
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
            SELECT id, username, display_name, role, is_active, created_at, last_login_at, password_hash, password_salt
            FROM users
            WHERE normalized_username = $username";
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

            storedHash = reader.GetString(7);
            storedSalt = reader.GetString(8);
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
            SELECT id, username, display_name, role, is_active, created_at, last_login_at
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
            SELECT id, username, display_name, role, is_active, created_at, last_login_at
            FROM users
            WHERE normalized_username = $username";
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
                u.display_name,
                u.role,
                u.is_active,
                u.created_at,
                u.last_login_at,
                COUNT(r.id) AS report_count
            FROM users u
            LEFT JOIN reports r ON lower(r.author) = u.normalized_username
            GROUP BY u.id, u.username, u.display_name, u.role, u.is_active, u.created_at, u.last_login_at
            ORDER BY CASE WHEN u.role = 'Admin' THEN 0 ELSE 1 END, u.display_name, u.username";

        using var reader = command.ExecuteReader();
        while (reader.Read())
        {
            users.Add(new AppUserSummary
            {
                Id = reader.GetInt32(0),
                Username = reader.GetString(1),
                DisplayName = reader.GetString(2),
                Role = reader.GetString(3),
                IsActive = reader.GetInt64(4) == 1,
                CreatedAt = ParseDateTime(reader.GetString(5)),
                LastLoginAt = reader.IsDBNull(6) ? null : ParseDateTime(reader.GetString(6)),
                ReportCount = reader.GetInt32(7)
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
                u.display_name,
                u.role,
                u.is_active,
                u.created_at,
                u.last_login_at,
                COUNT(r.id) AS report_count
            FROM users u
            LEFT JOIN reports r ON lower(r.author) = u.normalized_username
            WHERE u.id = $id
            GROUP BY u.id, u.username, u.display_name, u.role, u.is_active, u.created_at, u.last_login_at";
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
                DisplayName = reader.GetString(2),
                Role = reader.GetString(3),
                IsActive = reader.GetInt64(4) == 1,
                CreatedAt = ParseDateTime(reader.GetString(5)),
                LastLoginAt = reader.IsDBNull(6) ? null : ParseDateTime(reader.GetString(6)),
                ReportCount = reader.GetInt32(7)
            };
        }

        detail.RecentReports = GetRecentReportsForAuthor(detail.Username, connection);
        return detail;
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
                ON users(normalized_username);";
        command.ExecuteNonQuery();
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
                    display_name = $display_name,
                    role = $role,
                    password_hash = $password_hash,
                    password_salt = $password_salt,
                    is_active = 1
                WHERE id = $id";
            update.Parameters.AddWithValue("$id", existingId.Value);
            update.Parameters.AddWithValue("$plain_username", seedUser.Username.Trim());
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
                INSERT INTO users(username, normalized_username, display_name, role, password_hash, password_salt, is_active, created_at)
                VALUES($plain_username, $normalized_username, $display_name, $role, $password_hash, $password_salt, 1, $created_at)";
            insert.Parameters.AddWithValue("$plain_username", seedUser.Username.Trim());
            insert.Parameters.AddWithValue("$normalized_username", normalizedUsername);
            insert.Parameters.AddWithValue("$display_name", displayName);
            insert.Parameters.AddWithValue("$role", role);
            insert.Parameters.AddWithValue("$password_hash", hash);
            insert.Parameters.AddWithValue("$password_salt", salt);
            insert.Parameters.AddWithValue("$created_at", DateTime.UtcNow.ToString("O"));
            insert.ExecuteNonQuery();
        }

        transaction.Commit();
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

    private static AppUser ReadUser(SqliteDataReader reader) =>
        new()
        {
            Id = reader.GetInt32(0),
            Username = reader.GetString(1),
            DisplayName = reader.GetString(2),
            Role = reader.GetString(3),
            IsActive = reader.GetInt64(4) == 1,
            CreatedAt = ParseDateTime(reader.GetString(5)),
            LastLoginAt = reader.IsDBNull(6) ? null : ParseDateTime(reader.GetString(6))
        };

    private static string NormalizeUsername(string username) =>
        username.Trim().ToLowerInvariant();

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
