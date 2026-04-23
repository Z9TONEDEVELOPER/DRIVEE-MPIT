using System.Net.Http.Json;
using System.Text.Json;
using DriveeDataSpace.DriveeDataSpace.Desktop.Models;

namespace DriveeDataSpace.DriveeDataSpace.Desktop.Services;

/// <summary>
/// Клиент для обращения к Web-бэкенду (DriveeDataSpace.Web).
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

    // ── Выполнить NL-запрос ────────────────────────────────────────────────
    public async Task<PipelineResult> RunQueryAsync(string text, CancellationToken ct = default)
    {
        var req = new { text };
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
}

public record SaveReportRequest(string Name, string UserQuery, string IntentJson, string Sql, string Visualization);