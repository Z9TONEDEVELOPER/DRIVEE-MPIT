using DriveeDataSpace.Core.Models;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;

namespace DriveeDataSpace.Core.Services;

public sealed class LlmSettingsService
{
    private const int BusyTimeoutMs = 5_000;
    private const string ProviderKey = "llm.provider";
    private const string GigaChatAuthorizationKey = "llm.gigachat.authorization_key";

    private readonly IConfiguration _configuration;
    private readonly string _dbPath;
    private readonly TenantContext _tenantContext;
    private readonly SecretProtector _secretProtector;

    public LlmSettingsService(
        IConfiguration configuration,
        IHostEnvironment environment,
        TenantContext tenantContext,
        SecretProtector secretProtector)
    {
        _configuration = configuration;
        _tenantContext = tenantContext;
        _secretProtector = secretProtector;
        _dbPath = DataPathResolver.Resolve(environment, configuration["Data:ReportsDb"], "Data/reports.db");

        var directory = Path.GetDirectoryName(_dbPath);
        if (!string.IsNullOrWhiteSpace(directory))
            Directory.CreateDirectory(directory);

        EnsureSchema();
    }

    public LlmSettings Get()
        => Get(_tenantContext.CompanyId);

    public LlmSettings Get(int companyId)
    {
        var stored = ReadSetting(ScopedKey(companyId, ProviderKey));
        if (stored.Value == null && NormalizeCompanyId(companyId) == CompanyDefaults.DefaultCompanyId)
            stored = ReadSetting(ProviderKey);

        var provider = LlmProviders.Normalize(stored.Value ?? _configuration["Llm:Provider"]);
        return BuildSettings(provider, stored.UpdatedAt ?? DateTime.UtcNow, companyId);
    }

    public string GetProvider() => Get().Provider;

    public string? GetGigaChatAuthorizationKey()
        => GetGigaChatAuthorizationKey(_tenantContext.CompanyId);

    public string? GetGigaChatAuthorizationKey(int companyId)
    {
        var stored = ReadSetting(ScopedKey(companyId, GigaChatAuthorizationKey));
        if (stored.Value == null && NormalizeCompanyId(companyId) == CompanyDefaults.DefaultCompanyId)
            stored = ReadSetting(GigaChatAuthorizationKey);

        return string.IsNullOrWhiteSpace(stored.Value)
            ? _configuration["Llm:Fallback:GigaChat:AuthorizationKey"]
            : _secretProtector.Unprotect(stored.Value);
    }

    public LlmSettings SetProvider(string? provider)
        => SetProvider(provider, _tenantContext.CompanyId);

    public LlmSettings SetProvider(string? provider, int companyId)
    {
        var normalized = LlmProviders.Normalize(provider);
        if (string.Equals(normalized, LlmProviders.GigaChat, StringComparison.OrdinalIgnoreCase) &&
            string.IsNullOrWhiteSpace(GetGigaChatAuthorizationKey(companyId)))
        {
            throw new InvalidOperationException("GigaChat provider cannot be selected because the authorization key is not configured.");
        }

        var now = DateTime.UtcNow;
        WriteSetting(ScopedKey(companyId, ProviderKey), normalized, now);
        return BuildSettings(normalized, now, companyId);
    }

    public LlmSettings Save(UpdateLlmSettingsRequest request)
        => Save(request, _tenantContext.CompanyId);

    public LlmSettings Save(UpdateLlmSettingsRequest request, int companyId)
    {
        if (request.ClearGigaChatAuthorizationKey)
            ClearGigaChatAuthorizationKey(companyId);

        if (!string.IsNullOrWhiteSpace(request.GigaChatAuthorizationKey))
            SetGigaChatAuthorizationKey(request.GigaChatAuthorizationKey, companyId);

        return SetProvider(request.Provider, companyId);
    }

    public LlmSettings SetGigaChatAuthorizationKey(string authorizationKey, int companyId)
    {
        if (string.IsNullOrWhiteSpace(authorizationKey))
            throw new InvalidOperationException("Укажите GigaChat AuthorizationKey.");

        var now = DateTime.UtcNow;
        WriteSetting(
            ScopedKey(companyId, GigaChatAuthorizationKey),
            _secretProtector.Protect(authorizationKey.Trim()),
            now);
        return Get(companyId);
    }

    public void ClearGigaChatAuthorizationKey(int companyId)
    {
        using var connection = OpenConnection();
        using var command = connection.CreateCommand();
        command.CommandText = "DELETE FROM app_settings WHERE key = $key";
        command.Parameters.AddWithValue("$key", ScopedKey(companyId, GigaChatAuthorizationKey));
        command.ExecuteNonQuery();
    }

    private void WriteSetting(string key, string value, DateTime updatedAt)
    {
        if (string.IsNullOrWhiteSpace(value))
            throw new InvalidOperationException("Setting value cannot be empty.");

        using var connection = OpenConnection();
        using var command = connection.CreateCommand();
        command.CommandText = @"
            INSERT INTO app_settings(key, value, updated_at)
            VALUES($key, $value, $updated_at)
            ON CONFLICT(key) DO UPDATE SET
                value = excluded.value,
                updated_at = excluded.updated_at;";
        command.Parameters.AddWithValue("$key", key);
        command.Parameters.AddWithValue("$value", value);
        command.Parameters.AddWithValue("$updated_at", updatedAt.ToString("O"));
        command.ExecuteNonQuery();
    }

    private LlmSettings BuildSettings(string provider, DateTime updatedAt, int companyId) =>
        new(
            provider,
            _configuration["Llm:Endpoint"] ?? "http://localhost:1234/api/v1/chat",
            _configuration["Llm:Model"] ?? "qwen2.5-coder-7b-instruct",
            _configuration["Llm:Fallback:GigaChat:Model"] ?? "GigaChat",
            !string.IsNullOrWhiteSpace(GetGigaChatAuthorizationKey(companyId)),
            updatedAt);

    private (string? Value, DateTime? UpdatedAt) ReadSetting(string key)
    {
        using var connection = OpenConnection();
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT value, updated_at FROM app_settings WHERE key = $key";
        command.Parameters.AddWithValue("$key", key);

        using var reader = command.ExecuteReader();
        if (!reader.Read())
            return (null, null);

        var value = reader.IsDBNull(0) ? null : reader.GetString(0);
        var updatedAt = reader.IsDBNull(1) || !DateTime.TryParse(reader.GetString(1), out var parsed)
            ? (DateTime?)null
            : parsed;
        return (value, updatedAt);
    }

    private static int NormalizeCompanyId(int companyId) =>
        companyId <= 0 ? CompanyDefaults.DefaultCompanyId : companyId;

    private static string ScopedKey(int companyId, string key) =>
        $"company:{NormalizeCompanyId(companyId)}:{key}";

    private void EnsureSchema()
    {
        using var connection = OpenConnection();
        using var journalCommand = connection.CreateCommand();
        journalCommand.CommandText = "PRAGMA journal_mode = WAL;";
        _ = journalCommand.ExecuteScalar();

        using var command = connection.CreateCommand();
        command.CommandText = @"
            CREATE TABLE IF NOT EXISTS app_settings (
                key        TEXT PRIMARY KEY,
                value      TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );";
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
