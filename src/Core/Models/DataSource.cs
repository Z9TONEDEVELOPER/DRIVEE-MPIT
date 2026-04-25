using System.Text.Json.Serialization;

namespace DriveeDataSpace.Core.Models;

public static class DataSourceProviders
{
    public const string Sqlite = "sqlite";
    public const string PostgreSql = "postgresql";
    public const string SqlServer = "sqlserver";
    public const string MySql = "mysql";

    public static readonly IReadOnlyList<string> All = new[]
    {
        Sqlite,
        PostgreSql,
        SqlServer,
        MySql
    };

    public static bool CanExecute(string provider) =>
        string.Equals(provider, Sqlite, StringComparison.OrdinalIgnoreCase) ||
        string.Equals(provider, PostgreSql, StringComparison.OrdinalIgnoreCase);
}

public sealed class CompanyDataSource
{
    public int Id { get; set; }
    public int CompanyId { get; set; } = CompanyDefaults.DefaultCompanyId;
    public string Name { get; set; } = "";
    public string Provider { get; set; } = DataSourceProviders.Sqlite;
    public string ConnectionString { get; set; } = "";
    public bool IsConnectionStringMasked { get; set; }
    public string? SemanticJson { get; set; }
    public string? SchemaJson { get; set; }
    public bool IsBuiltin { get; set; }
    public bool IsActive { get; set; }
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
    public DateTime UpdatedAt { get; set; } = DateTime.UtcNow;
    public DateTime? LastValidatedAt { get; set; }
    public string? LastValidationError { get; set; }
}

public sealed class DataSourceInput
{
    public int? Id { get; set; }
    public string Name { get; set; } = "";
    public string Provider { get; set; } = DataSourceProviders.Sqlite;
    public string ConnectionString { get; set; } = "";
    public string? SemanticJson { get; set; }
    public bool MakeActive { get; set; }
}

public sealed class DataSourceTestResult
{
    public bool Ok { get; set; }
    public string Message { get; set; } = "";
    public DateTime CheckedAt { get; set; } = DateTime.UtcNow;
}

public sealed class SchemaInspectionResult
{
    public string Provider { get; set; } = DataSourceProviders.Sqlite;
    public List<SchemaTableInfo> Tables { get; set; } = new();
}

public sealed class SchemaTableInfo
{
    public string SchemaName { get; set; } = "";
    public string Name { get; set; } = "";
    public long? EstimatedRows { get; set; }
    public List<SchemaColumnInfo> Columns { get; set; } = new();
}

public sealed class SchemaColumnInfo
{
    public string Name { get; set; } = "";
    public string DataType { get; set; } = "";
    public bool IsNullable { get; set; }
    public bool IsPrimaryKey { get; set; }
}

public sealed class SemanticLayerJson
{
    [JsonPropertyName("sources")]
    public List<SemanticSourceJson> Sources { get; set; } = new();

    [JsonPropertyName("metrics")]
    public List<SemanticMetricJson> Metrics { get; set; } = new();

    [JsonPropertyName("dimensions")]
    public List<SemanticDimensionJson> Dimensions { get; set; } = new();

    [JsonPropertyName("filters")]
    public List<SemanticFilterJson> Filters { get; set; } = new();

    [JsonPropertyName("presets")]
    public List<SemanticPresetJson> Presets { get; set; } = new();
}

public sealed class SemanticSourceJson
{
    [JsonPropertyName("key")]
    public string Key { get; set; } = "";

    [JsonPropertyName("table")]
    public string Table { get; set; } = "";

    [JsonPropertyName("display_label")]
    public string DisplayLabel { get; set; } = "";

    [JsonPropertyName("allowed_columns")]
    public List<string> AllowedColumns { get; set; } = new();
}

public sealed class SemanticMetricJson
{
    [JsonPropertyName("key")]
    public string Key { get; set; } = "";

    [JsonPropertyName("display_label")]
    public string DisplayLabel { get; set; } = "";

    [JsonPropertyName("aggregation")]
    public string Aggregation { get; set; } = "";

    [JsonPropertyName("expression")]
    public string Expression { get; set; } = "";

    [JsonPropertyName("source")]
    public string Source { get; set; } = "";

    [JsonPropertyName("date_column")]
    public string DateColumn { get; set; } = "";

    [JsonPropertyName("allowed_dimensions")]
    public List<string> AllowedDimensions { get; set; } = new();

    [JsonPropertyName("allowed_filters")]
    public List<string> AllowedFilters { get; set; } = new();

    [JsonPropertyName("default_filters")]
    public List<IntentFilter> DefaultFilters { get; set; } = new();

    [JsonPropertyName("synonyms")]
    public List<string> Synonyms { get; set; } = new();
}

public sealed class SemanticDimensionJson
{
    [JsonPropertyName("key")]
    public string Key { get; set; } = "";

    [JsonPropertyName("display_label")]
    public string DisplayLabel { get; set; } = "";

    [JsonPropertyName("expression")]
    public string Expression { get; set; } = "";

    [JsonPropertyName("is_time_dimension")]
    public bool IsTimeDimension { get; set; }

    [JsonPropertyName("source")]
    public string Source { get; set; } = "";

    [JsonPropertyName("synonyms")]
    public List<string> Synonyms { get; set; } = new();
}

public sealed class SemanticFilterJson
{
    [JsonPropertyName("key")]
    public string Key { get; set; } = "";

    [JsonPropertyName("display_label")]
    public string DisplayLabel { get; set; } = "";

    [JsonPropertyName("column")]
    public string Column { get; set; } = "";

    [JsonPropertyName("source")]
    public string Source { get; set; } = "";

    [JsonPropertyName("allowed_operators")]
    public List<string> AllowedOperators { get; set; } = new();

    [JsonPropertyName("value_aliases")]
    public Dictionary<string, List<string>> ValueAliases { get; set; } = new(StringComparer.OrdinalIgnoreCase);

    [JsonPropertyName("synonyms")]
    public List<string> Synonyms { get; set; } = new();
}

public sealed class SemanticPresetJson
{
    [JsonPropertyName("key")]
    public string Key { get; set; } = "";

    [JsonPropertyName("phrases")]
    public List<string> Phrases { get; set; } = new();

    [JsonPropertyName("metric")]
    public string? Metric { get; set; }

    [JsonPropertyName("dimension")]
    public string? Dimension { get; set; }

    [JsonPropertyName("filters")]
    public List<IntentFilter> Filters { get; set; } = new();

    [JsonPropertyName("visualization")]
    public string? Visualization { get; set; }
}
