using System.Text.Json.Serialization;

namespace DriveeDataSpace.DriveeDataSpace.Desktop.Models;

// ── Запрос ──────────────────────────────────────────────────────────────────
public record QueryRequest(string Text);

// ── Intent (совпадает с Web QueryIntent) ─────────────────────────────────────
public class QueryIntent
{
    [JsonPropertyName("kind")]               public string Kind              { get; set; } = "query";
    [JsonPropertyName("reply")]              public string? Reply             { get; set; }
    [JsonPropertyName("clarification")]      public string? Clarification     { get; set; }
    [JsonPropertyName("intent")]             public string Intent             { get; set; } = "metric_query";
    [JsonPropertyName("metric")]             public string? Metric            { get; set; }
    [JsonPropertyName("aggregation")]        public string? Aggregation       { get; set; }
    [JsonPropertyName("dimensions")]         public List<string> Dimensions   { get; set; } = new();
    [JsonPropertyName("filters")]            public List<IntentFilter> Filters { get; set; } = new();
    [JsonPropertyName("date_range")]         public QueryDateRange? DateRange { get; set; }
    [JsonPropertyName("sort")]               public List<QuerySort> Sort      { get; set; } = new();
    [JsonPropertyName("limit")]              public int? Limit                { get; set; }
    [JsonPropertyName("visualization")]      public string? Visualization     { get; set; }
    [JsonPropertyName("source")]             public string? Source            { get; set; }
    [JsonPropertyName("confidence")]         public double Confidence         { get; set; } = 0.5;
    [JsonPropertyName("explanation")]        public string? Explanation       { get; set; }
    [JsonPropertyName("group_by")]           public string? GroupBy           { get; set; }
    [JsonPropertyName("period")]             public string? Period            { get; set; }
    [JsonPropertyName("periods")]            public List<string>? Periods     { get; set; }
    [JsonPropertyName("visualization_hint")] public string? VisualizationHint { get; set; }
}

public class IntentFilter
{
    [JsonPropertyName("field")]    public string? Field    { get; set; }
    [JsonPropertyName("operator")] public string? Operator { get; set; }
    [JsonPropertyName("value")]    public object? Value    { get; set; }
}

public class QueryDateRange
{
    [JsonPropertyName("type")]        public string? Type       { get; set; }
    [JsonPropertyName("value")]       public int? Value         { get; set; }
    [JsonPropertyName("unit")]        public string? Unit       { get; set; }
    [JsonPropertyName("start")]       public string? Start      { get; set; }
    [JsonPropertyName("end")]         public string? End        { get; set; }
    [JsonPropertyName("date_column")] public string? DateColumn { get; set; }
    [JsonPropertyName("label")]       public string? Label      { get; set; }
}

public class QuerySort
{
    [JsonPropertyName("field")]     public string? Field     { get; set; }
    [JsonPropertyName("direction")] public string? Direction { get; set; }
}

// ── Результат запроса ────────────────────────────────────────────────────────
public class QueryResult
{
    [JsonPropertyName("columns")]    public List<string> Columns          { get; set; } = new();
    [JsonPropertyName("rows")]       public List<List<object?>> Rows      { get; set; } = new();
    [JsonPropertyName("rowCount")]   public int RowCount                  => Rows.Count;
    [JsonPropertyName("durationMs")] public long DurationMs               { get; set; }
}

// ── Шаг объяснения ───────────────────────────────────────────────────────────
public class ReasoningStep
{
    [JsonPropertyName("icon")]   public string Icon   { get; set; } = "";
    [JsonPropertyName("title")]  public string Title  { get; set; } = "";
    [JsonPropertyName("detail")] public string Detail { get; set; } = "";
    [JsonPropertyName("code")]   public string? Code  { get; set; }
}

// ── Полный ответ pipeline ─────────────────────────────────────────────────────
public class PipelineResult
{
    [JsonPropertyName("userQuery")]       public string UserQuery            { get; set; } = "";
    [JsonPropertyName("intent")]          public QueryIntent? Intent         { get; set; }
    [JsonPropertyName("sql")]             public string? Sql                 { get; set; }
    [JsonPropertyName("result")]          public QueryResult? Result         { get; set; }
    [JsonPropertyName("explain")]         public string? Explain             { get; set; }
    [JsonPropertyName("technicalExplain")]public string? TechnicalExplain   { get; set; }
    [JsonPropertyName("structuredExplain")]public QueryExplanation? StructuredExplain { get; set; }
    [JsonPropertyName("error")]           public string? Error               { get; set; }
    [JsonPropertyName("visualization")]   public string Visualization        { get; set; } = "table";
    [JsonPropertyName("confidence")]      public double Confidence           { get; set; }
    [JsonPropertyName("warnings")]        public List<string> Warnings       { get; set; } = new();
    [JsonPropertyName("isChat")]          public bool IsChat                 { get; set; }
    [JsonPropertyName("chatReply")]       public string? ChatReply           { get; set; }
    [JsonPropertyName("reasoningTrail")]  public List<ReasoningStep> ReasoningTrail { get; set; } = new();
}

public class QueryExplanation
{
    [JsonPropertyName("metricLabel")]        public string MetricLabel        { get; set; } = "";
    [JsonPropertyName("dimensionsLabel")]    public string DimensionsLabel    { get; set; } = "";
    [JsonPropertyName("periodLabel")]        public string PeriodLabel        { get; set; } = "";
    [JsonPropertyName("filtersLabel")]       public List<string> FiltersLabel { get; set; } = new();
    [JsonPropertyName("aggregationLabel")]   public string AggregationLabel   { get; set; } = "";
    [JsonPropertyName("visualizationLabel")] public string VisualizationLabel { get; set; } = "";
    [JsonPropertyName("sortLabel")]          public string SortLabel          { get; set; } = "";
    [JsonPropertyName("sourceLabel")]        public string SourceLabel        { get; set; } = "";
    [JsonPropertyName("limit")]              public int Limit                 { get; set; }
    [JsonPropertyName("confidence")]         public double Confidence         { get; set; }
}

// ── Отчёт ────────────────────────────────────────────────────────────────────
public class Report
{
    [JsonPropertyName("id")]            public int Id            { get; set; }
    [JsonPropertyName("name")]          public string Name       { get; set; } = "";
    [JsonPropertyName("userQuery")]     public string UserQuery  { get; set; } = "";
    [JsonPropertyName("intentJson")]    public string IntentJson { get; set; } = "";
    [JsonPropertyName("sql")]           public string Sql        { get; set; } = "";
    [JsonPropertyName("visualization")] public string Visualization { get; set; } = "table";
    [JsonPropertyName("author")]        public string Author     { get; set; } = "demo";
    [JsonPropertyName("createdAt")]     public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
}

// ── Сообщение чата ────────────────────────────────────────────────────────────
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
