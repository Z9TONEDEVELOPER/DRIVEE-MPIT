namespace NexusDataSpace.Core.Models;

public sealed record RejectRegistrationRequest(string? Reason);

public static class LlmProviders
{
    public const string Local = "Local";
    public const string GigaChat = "GigaChat";

    public static IReadOnlyList<string> All { get; } = new[] { Local, GigaChat };

    public static string Normalize(string? provider)
    {
        if (string.IsNullOrWhiteSpace(provider))
            return Local;

        var normalized = provider.Trim();
        if (string.Equals(normalized, Local, StringComparison.OrdinalIgnoreCase))
            return Local;
        if (string.Equals(normalized, GigaChat, StringComparison.OrdinalIgnoreCase))
            return GigaChat;

        throw new InvalidOperationException($"Unsupported LLM provider `{provider}`.");
    }
}

public static class StructuredOutputModes
{
    public const string JsonSchema = "json_schema";
    public const string JsonObject = "json_object";
    public const string Off = "off";

    public static string Normalize(string? mode, string fallback)
    {
        if (string.IsNullOrWhiteSpace(mode))
            return fallback;

        return mode.Trim().ToLowerInvariant() switch
        {
            JsonSchema or "schema" or "strict" => JsonSchema,
            JsonObject or "json" => JsonObject,
            Off or "none" or "disabled" or "false" => Off,
            _ => fallback
        };
    }
}

public sealed record LlmSettings(
    string Provider,
    string LocalEndpoint,
    string LocalModel,
    string GigaChatModel,
    bool IsGigaChatConfigured,
    DateTime UpdatedAt);

public sealed record UpdateLlmSettingsRequest(
    string Provider,
    string? GigaChatAuthorizationKey = null,
    bool ClearGigaChatAuthorizationKey = false);

public sealed record UpdateUserRoleRequest(string Role);

public sealed record UpdateUserActiveRequest(bool IsActive);

public sealed record SecurityChecklistItem(
    string Key,
    string Title,
    string Status,
    string Details,
    bool Passed);

public sealed record SecurityChecklist(IReadOnlyList<SecurityChecklistItem> Items)
{
    public int PassedCount => Items.Count(item => item.Passed);
    public int TotalCount => Items.Count;
    public bool IsReady => Items.All(item => item.Passed);
}

public sealed record SecretRotationResult(int RotatedCount, int SkippedCount);

public sealed record RetentionCleanupResult(int DeletedAuditEvents, int DeletedReports);
