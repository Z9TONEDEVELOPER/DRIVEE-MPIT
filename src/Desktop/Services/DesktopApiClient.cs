using System.Net.Http.Json;
using System.Text.Json;
using DriveeDataSpace.DriveeDataSpace.Desktop.Models;

namespace DriveeDataSpace.DriveeDataSpace.Desktop.Services;

public class DesktopApiClient
{
    private readonly HttpClient _http;
    private readonly JsonSerializerOptions _jsonOptions = new(JsonSerializerDefaults.Web)
    {
        PropertyNameCaseInsensitive = true
    };

    public DesktopApiClient(HttpClient http) => _http = http;

    public async Task<PipelineResult> ExecuteQueryAsync(string queryText, CancellationToken ct = default)
    {
        var request = new QueryRequest(queryText);
        var response = await _http.PostAsJsonAsync("/api/query", request, _jsonOptions, ct);
        response.EnsureSuccessStatusCode();
        return (await response.Content.ReadFromJsonAsync<PipelineResult>(_jsonOptions, ct))!;
    }

    public async Task<List<Report>> GetReportsAsync(CancellationToken ct = default)
    {
        return (await _http.GetFromJsonAsync<List<Report>>("/api/reports", _jsonOptions, ct)) ?? new();
    }

    public async Task<Report> SaveReportAsync(Report report, CancellationToken ct = default)
    {
        var response = await _http.PostAsJsonAsync("/api/reports", report, _jsonOptions, ct);
        response.EnsureSuccessStatusCode();
        return (await response.Content.ReadFromJsonAsync<Report>(_jsonOptions, ct))!;
    }

    public async Task<PipelineResult> RerunReportAsync(string reportId, CancellationToken ct = default)
    {
        var response = await _http.PostAsync($"/api/reports/{reportId}/rerun", null, ct);
        response.EnsureSuccessStatusCode();
        return (await response.Content.ReadFromJsonAsync<PipelineResult>(_jsonOptions, ct))!;
    }
}