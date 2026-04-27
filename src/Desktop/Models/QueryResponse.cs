using NexusDataSpace.Core.Models;

namespace NexusDataSpace.Desktop.Models;

public enum ChatRole { User, Bot, Result }

public class ChatMessage
{
    public string Id            { get; } = Guid.NewGuid().ToString("N");
    public ChatRole Role        { get; set; }
    public string? Text         { get; set; }
    public PipelineResult? Result { get; set; }
    public string Visualization { get; set; } = "table";
    public string ReportName    { get; set; } = "";
    public string? SavedAs      { get; set; }
    public string? InsightTab   { get; set; }  // "reasoning" | "sql" | null
    public bool DetailsOpen     { get; set; }
}
