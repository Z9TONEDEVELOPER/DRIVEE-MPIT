using NexusDataSpace.Core.Models;

namespace NexusDataSpace.Web.Models;

public class WorkspaceChatMessage
{
    public string Id { get; } = Guid.NewGuid().ToString("N");
    public string Role { get; set; } = "user";
    public string? Text { get; set; }
    public PipelineResult? Result { get; set; }
    public string Visualization { get; set; } = "table";
    public string ReportName { get; set; } = "";
    public string? SavedAs { get; set; }
    public string? InsightTab { get; set; }
    public bool DetailsOpen { get; set; }
    public bool CorrectionOpen { get; set; }
    public string CorrectionMetric { get; set; } = "";
    public string CorrectionDimension { get; set; } = "";
    public string CorrectionPeriod { get; set; } = "";
    public string CorrectionVisualization { get; set; } = "";
    public string? CorrectionError { get; set; }
}
