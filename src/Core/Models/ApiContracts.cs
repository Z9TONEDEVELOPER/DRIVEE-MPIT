namespace DriveeDataSpace.Core.Models;

public sealed record QueryRequest(
    string Text,
    IReadOnlyList<ChatTurn>? History = null,
    QueryIntent? PreviousIntent = null);

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
    string Username,
    string DisplayName,
    string Role,
    DateTimeOffset ExpiresAt);
