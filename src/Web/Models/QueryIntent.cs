using System.Text.Json.Serialization;

namespace DriveeDataSpace.Web.Models;

public class QueryIntent
{
    [JsonPropertyName("kind")] public string Kind { get; set; } = "query";
    [JsonPropertyName("reply")] public string? Reply { get; set; }
    [JsonPropertyName("intent")] public string Intent { get; set; } = "aggregate";
    [JsonPropertyName("metric")] public string Metric { get; set; } = "orders";
    [JsonPropertyName("group_by")] public string? GroupBy { get; set; }
    [JsonPropertyName("period")] public string? Period { get; set; }
    [JsonPropertyName("periods")] public List<string>? Periods { get; set; }
    [JsonPropertyName("filters")] public Dictionary<string, string>? Filters { get; set; }
    [JsonPropertyName("visualization_hint")] public string VisualizationHint { get; set; } = "table";
    [JsonPropertyName("confidence")] public double Confidence { get; set; } = 0.5;
    [JsonPropertyName("explanation")] public string? Explanation { get; set; }
}
