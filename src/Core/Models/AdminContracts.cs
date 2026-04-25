namespace DriveeDataSpace.Core.Models;

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

public sealed record LlmSettings(
    string Provider,
    string LocalEndpoint,
    string LocalModel,
    string GigaChatModel,
    bool IsGigaChatConfigured,
    DateTime UpdatedAt);

public sealed record UpdateLlmSettingsRequest(string Provider);
