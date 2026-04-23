using System.Net.Http.Json;
using System.Text.Json;
using DriveeDataSpace.Desktop.Models;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;
namespace DriveeDataSpace.Desktop.Services;

public class DesktopApiClient
{
    private readonly HttpClient _http;
    private readonly JsonSerializerOptions _jsonOptions = new(JsonSerializerDefaults.Web)
    {
        PropertyNameCaseInsensitive = true
    };

    public DesktopApiClient(HttpClient http) => _http = http;

    public async Task<QueryResponse> ExecuteQueryAsync(string queryText, CancellationToken ct = default)
    {
        var request = new QueryRequest(queryText);
        var response = await _http.PostAsJsonAsync("/api/query", request, _jsonOptions, ct);
        response.EnsureSuccessStatusCode();
        return (await response.Content.ReadFromJsonAsync<QueryResponse>(_jsonOptions, ct))!;
    }

    public async Task<List<QueryResponse>> GetReportsAsync(CancellationToken ct = default)
    {
        return (await _http.GetFromJsonAsync<List<QueryResponse>>("/api/reports", _jsonOptions, ct)) ?? new();
    }

    public async Task<QueryResponse> SaveReportAsync(QueryResponse report, CancellationToken ct = default)
    {
        var response = await _http.PostAsJsonAsync("/api/reports", report, _jsonOptions, ct);
        response.EnsureSuccessStatusCode();
        return (await response.Content.ReadFromJsonAsync<QueryResponse>(_jsonOptions, ct))!;
    }

    public async Task<QueryResponse> RerunReportAsync(string reportId, CancellationToken ct = default)
    {
        var response = await _http.PostAsync($"/api/reports/{reportId}/rerun", null, ct);
        response.EnsureSuccessStatusCode();
        return (await response.Content.ReadFromJsonAsync<QueryResponse>(_jsonOptions, ct))!;
    }
}