using DriveeDataSpace.Core.Models;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;

namespace DriveeDataSpace.Core.Services;

public sealed class SecurityChecklistService
{
    private readonly IConfiguration _configuration;
    private readonly IHostEnvironment _environment;
    private readonly LlmSettingsService _llmSettings;

    public SecurityChecklistService(
        IConfiguration configuration,
        IHostEnvironment environment,
        LlmSettingsService llmSettings)
    {
        _configuration = configuration;
        _environment = environment;
        _llmSettings = llmSettings;
    }

    public SecurityChecklist Get(int companyId)
    {
        const string authDevDefaultKey = "drivee-bi-local-dev-token-signing-key-change-me";
        const string secretDevDefaultKey = "drivee-bi-local-dev-secret-protection-key-change-me";

        var authKey = _configuration["Auth:ApiTokenSigningKey"] ?? "";
        var secretKey = _configuration["Security:SecretProtectionKey"] ?? "";
        var hasGigaChatKey = !string.IsNullOrWhiteSpace(_llmSettings.GetGigaChatAuthorizationKey(companyId));
        var loginLimit = ReadBoundedInt(_configuration, "Security:LoginAttemptsPerMinute", 5, 1, 1_000);
        var ipLoginLimit = ReadBoundedInt(_configuration, "Security:LoginIpAttemptsPerMinute", 30, 1, 10_000);
        var metadataProvider = _configuration["Data:MetadataProvider"] ?? "sqlite";

        var items = new List<SecurityChecklistItem>
        {
            BuildItem(
                "environment",
                "Production environment",
                _environment.IsProduction(),
                _environment.IsProduction()
                    ? "ASPNETCORE_ENVIRONMENT=Production."
                    : $"Сейчас окружение `{_environment.EnvironmentName}`. Для прода включите Production."),
            BuildItem(
                "auth-signing-key",
                "Auth signing key",
                authKey.Length >= 32 && !string.Equals(authKey, authDevDefaultKey, StringComparison.Ordinal),
                authKey.Length >= 32 && !string.Equals(authKey, authDevDefaultKey, StringComparison.Ordinal)
                    ? "Ключ подписи токенов задан и не похож на dev-default."
                    : "Задайте Auth:ApiTokenSigningKey длиной 32+ символа из secret storage."),
            BuildItem(
                "secret-protection-key",
                "Secret protection key",
                secretKey.Length >= 32 && !string.Equals(secretKey, secretDevDefaultKey, StringComparison.Ordinal),
                secretKey.Length >= 32 && !string.Equals(secretKey, secretDevDefaultKey, StringComparison.Ordinal)
                    ? "Отдельный ключ шифрования секретов задан."
                    : "Задайте Security:SecretProtectionKey отдельно от Auth:ApiTokenSigningKey."),
            BuildItem(
                "login-rate-limit",
                "Login rate limit",
                loginLimit <= 10 && ipLoginLimit <= 60,
                $"Текущие лимиты: user={loginLimit}/window, ip={ipLoginLimit}/window."),
            BuildItem(
                "metadata-postgresql",
                "Metadata DB",
                string.Equals(metadataProvider, "postgresql", StringComparison.OrdinalIgnoreCase),
                string.Equals(metadataProvider, "postgresql", StringComparison.OrdinalIgnoreCase)
                    ? "Metadata provider настроен как PostgreSQL."
                    : "Для production задайте Data:MetadataProvider=postgresql. Сейчас metadata-сервисы работают на SQLite."),
            BuildItem(
                "gigachat-secret",
                "LLM secret storage",
                hasGigaChatKey,
                hasGigaChatKey
                    ? "GigaChat AuthorizationKey сохранён через защищённое хранилище/config."
                    : "Если используете GigaChat, добавьте AuthorizationKey через UI или secret storage.")
        };

        return new SecurityChecklist(items);
    }

    private static SecurityChecklistItem BuildItem(string key, string title, bool passed, string details) =>
        new(key, title, passed ? "ok" : "attention", details, passed);

    private static int ReadBoundedInt(IConfiguration configuration, string key, int fallback, int min, int max) =>
        int.TryParse(configuration[key], out var value) ? Math.Clamp(value, min, max) : fallback;
}
