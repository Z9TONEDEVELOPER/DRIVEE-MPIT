namespace NexusDataSpace.Core.Models;

public class QueryResult
{
    public List<string> Columns { get; set; } = new();
    public List<List<object?>> Rows { get; set; } = new();
    public int RowCount => Rows.Count;
    public long DurationMs { get; set; }
}

public record ReasoningStep(string Icon, string Title, string Detail, string? Code = null);

public class QueryExplanation
{
    public string MetricLabel { get; set; } = "";
    public string DimensionsLabel { get; set; } = "";
    public string PeriodLabel { get; set; } = "";
    public List<string> FiltersLabel { get; set; } = new();
    public string AggregationLabel { get; set; } = "";
    public string VisualizationLabel { get; set; } = "";
    public string SortLabel { get; set; } = "";
    public string SourceLabel { get; set; } = "";
    public int Limit { get; set; }
    public double Confidence { get; set; }
}

public class PipelineResult
{
    public string UserQuery { get; set; } = "";
    public QueryIntent? Intent { get; set; }
    public string? Sql { get; set; }
    public QueryResult? Result { get; set; }
    public string? Explain { get; set; }
    public string? TechnicalExplain { get; set; }
    public QueryExplanation? StructuredExplain { get; set; }
    public string? Error { get; set; }
    public string Visualization { get; set; } = "table";
    public double Confidence { get; set; }
    public List<string> Warnings { get; set; } = new();
    public bool IsChat { get; set; }
    public string? ChatReply { get; set; }
    public List<ReasoningStep> ReasoningTrail { get; set; } = new();
}
