using System.Text.Json.Serialization;
using System.Collections.Generic;
using CommunityToolkit.Mvvm.ComponentModel;
using System;
using ScottPlot;
namespace DriveeDataSpace.Desktop.Models;

public record QueryRequest(string Text);

public record QueryResponse
{
    public string Id { get; init; } = Guid.NewGuid().ToString();
    public QueryIntentDto Intent { get; init; } = null!;
    public string FinalSql { get; init; } = string.Empty;
    public List<Dictionary<string, object>> ResultTable { get; init; } = new();
    public ExplainDto Explain { get; init; } = null!;
    public string VisualizationHint { get; init; } = "bar";
    public ScottPlot.Plot? PlotModel { get; set; } = new();
}

public record QueryIntentDto(
    string Intent,
    string Metric,
    string Aggregation,
    string[]? Periods,
    double Confidence,
    string? GroupBy = null);

public record ExplainDto(
    string BusinessExplanation,
    string TechnicalExplanation,
    string TemplateUsed);