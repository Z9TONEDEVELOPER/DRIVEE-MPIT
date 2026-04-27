using System.Net.Http.Json;
using System.Net.Http.Headers;
using System.Text.Json;
using NexusDataSpace.Core.Models;

namespace NexusDataSpace.Desktop.Services;

/// <summary>
/// Клиент для обращения к Web-бэкенду (NexusDataSpace.Web).
/// Desktop не содержит бизнес-логики — всё делает Web API.
/// </summary>
public class BiApiClient
{
    private readonly HttpClient _http;
    private readonly JsonSerializerOptions _json = new(JsonSerializerDefaults.Web)
    {
        PropertyNameCaseInsensitive = true
    };

    public BiApiClient(HttpClient http) => _http = http;

    public AuthSession? CurrentSession { get; private set; }

    public bool IsAuthenticated => CurrentSession?.ExpiresAt > DateTimeOffset.UtcNow;

    public async Task<AuthSession> LoginAsync(string username, string password, CancellationToken ct = default)
    {
        var resp = await _http.PostAsJsonAsync("/api/auth/login", new LoginRequest(username, password), _json, ct);
        resp.EnsureSuccessStatusCode();

        CurrentSession = (await resp.Content.ReadFromJsonAsync<AuthSession>(_json, ct))!;
        _http.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", CurrentSession.AccessToken);
        return CurrentSession;
    }

    public void Logout()
    {
        CurrentSession = null;
        _http.DefaultRequestHeaders.Authorization = null;
    }

    // ── Выполнить NL-запрос ────────────────────────────────────────────────
    public async Task<PipelineResult> RunQueryAsync(
        string text,
        IReadOnlyList<ChatTurn>? history = null,
        QueryIntent? previousIntent = null,
        CancellationToken ct = default)
    {
        var req = new QueryRequest(text, history, previousIntent);
        var resp = await _http.PostAsJsonAsync("/api/query", req, _json, ct);
        resp.EnsureSuccessStatusCode();
        return (await resp.Content.ReadFromJsonAsync<PipelineResult>(_json, ct))!;
    }

    // ── Отчёты ────────────────────────────────────────────────────────────
    public async Task<List<Report>> GetReportsAsync(CancellationToken ct = default)
        => (await _http.GetFromJsonAsync<List<Report>>("/api/reports", _json, ct)) ?? new();

    public async Task<Report> SaveReportAsync(SaveReportRequest req, CancellationToken ct = default)
    {
        var resp = await _http.PostAsJsonAsync("/api/reports", req, _json, ct);
        resp.EnsureSuccessStatusCode();
        return (await resp.Content.ReadFromJsonAsync<Report>(_json, ct))!;
    }

    public async Task<PipelineResult> RerunReportAsync(int reportId, CancellationToken ct = default)
    {
        var resp = await _http.PostAsync($"/api/reports/{reportId}/rerun", null, ct);
        resp.EnsureSuccessStatusCode();
        return (await resp.Content.ReadFromJsonAsync<PipelineResult>(_json, ct))!;
    }

    public async Task DeleteReportAsync(int reportId, CancellationToken ct = default)
    {
        await _http.DeleteAsync($"/api/reports/{reportId}", ct);
    }

    public async Task<List<AppUserSummary>> GetUsersAsync(CancellationToken ct = default)
        => (await _http.GetFromJsonAsync<List<AppUserSummary>>("/api/admin/users", _json, ct)) ?? new();

    public async Task<Company> GetCompanyAsync(CancellationToken ct = default)
        => (await _http.GetFromJsonAsync<Company>("/api/admin/company", _json, ct))!;

    public async Task<List<RegistrationRequest>> GetPendingRegistrationsAsync(CancellationToken ct = default)
        => (await _http.GetFromJsonAsync<List<RegistrationRequest>>(
            $"/api/admin/registration-requests?status={RegistrationRequestStatuses.Pending}",
            _json,
            ct)) ?? new();

    public async Task<LlmSettings> GetLlmSettingsAsync(CancellationToken ct = default)
        => (await _http.GetFromJsonAsync<LlmSettings>("/api/admin/llm-settings", _json, ct))!;

    public async Task<LlmSettings> UpdateLlmSettingsAsync(string provider, string? gigaChatAuthorizationKey = null, CancellationToken ct = default)
    {
        var resp = await _http.PostAsJsonAsync(
            "/api/admin/llm-settings",
            new UpdateLlmSettingsRequest(provider, gigaChatAuthorizationKey),
            _json,
            ct);
        resp.EnsureSuccessStatusCode();
        return (await resp.Content.ReadFromJsonAsync<LlmSettings>(_json, ct))!;
    }

    public async Task<RegistrationDecisionResult> ApproveRegistrationAsync(int id, CancellationToken ct = default)
    {
        var resp = await _http.PostAsync($"/api/admin/registration-requests/{id}/approve", null, ct);
        resp.EnsureSuccessStatusCode();
        return (await resp.Content.ReadFromJsonAsync<RegistrationDecisionResult>(_json, ct))!;
    }

    public async Task<RegistrationDecisionResult> RejectRegistrationAsync(int id, string? reason, CancellationToken ct = default)
    {
        var resp = await _http.PostAsJsonAsync(
            $"/api/admin/registration-requests/{id}/reject",
            new RejectRegistrationRequest(reason),
            _json,
            ct);
        resp.EnsureSuccessStatusCode();
        return (await resp.Content.ReadFromJsonAsync<RegistrationDecisionResult>(_json, ct))!;
    }
}
