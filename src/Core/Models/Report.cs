namespace DriveeDataSpace.Core.Models;

public class Report
{
    public int Id { get; set; }
    public string Name { get; set; } = "";
    public string UserQuery { get; set; } = "";
    public string IntentJson { get; set; } = "";
    public string Sql { get; set; } = "";
    public string Visualization { get; set; } = "table";
    public string Author { get; set; } = "demo";
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
}
