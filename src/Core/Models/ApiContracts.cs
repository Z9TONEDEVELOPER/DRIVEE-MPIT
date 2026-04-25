namespace DriveeDataSpace.Core.Models;

public sealed record QueryRequest(
    string Text,
    IReadOnlyList<ChatTurn>? History = null,
    QueryIntent? PreviousIntent = null);

public sealed record QueryJobSubmitRequest(
    string Text,
    IReadOnlyList<ChatTurn>? History = null,
    QueryIntent? PreviousIntent = null);

public static class QueryJobStatuses
{
    public const string Queued = "queued";
    public const string Running = "running";
    public const string Succeeded = "succeeded";
    public const string Failed = "failed";
    public const string Canceled = "canceled";
}

public sealed record QueryJobSnapshot(
    string Id,
    string Status,
    string Text,
    DateTimeOffset CreatedAt,
    DateTimeOffset? StartedAt,
    DateTimeOffset? CompletedAt,
    PipelineResult? Result,
    string? Error);

public sealed record SaveReportRequest(
    string Name,
    string UserQuery,
    string IntentJson,
    string Sql,
    string Visualization);

public sealed record LoginRequest(string Username, string Password);

public sealed record AuthSession(
    string AccessToken,
    DateTimeOffset ExpiresAt,
    AppUser User);

public sealed record AuthUserSession(
    int Id,
    int CompanyId,
    string Username,
    string DisplayName,
    string Role,
    DateTimeOffset ExpiresAt);
