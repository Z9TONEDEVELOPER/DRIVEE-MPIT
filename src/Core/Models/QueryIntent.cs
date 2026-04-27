using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace NexusDataSpace.Core.Models;

public sealed class QueryIntent
{
    [JsonPropertyName("kind")]
    public string Kind { get; set; } = QueryIntentKinds.Query;

    [JsonPropertyName("reply")]
    public string? Reply { get; set; }

    [JsonPropertyName("clarification")]
    public string? Clarification { get; set; }

    [JsonPropertyName("intent")]
    public string Intent { get; set; } = QueryIntentKinds.MetricQuery;

    [JsonPropertyName("metric")]
    public string? Metric { get; set; }

    [JsonPropertyName("aggregation")]
    public string? Aggregation { get; set; }

    [JsonPropertyName("dimensions")]
    public List<string> Dimensions { get; set; } = new();

    [JsonPropertyName("filters")]
    [JsonConverter(typeof(IntentFilterListJsonConverter))]
    public List<IntentFilter> Filters { get; set; } = new();

    [JsonPropertyName("date_range")]
    public QueryDateRange? DateRange { get; set; }

    [JsonPropertyName("sort")]
    public List<QuerySort> Sort { get; set; } = new();

    [JsonPropertyName("limit")]
    public int? Limit { get; set; }

    [JsonPropertyName("visualization")]
    public string? Visualization { get; set; }

    [JsonPropertyName("source")]
    public string? Source { get; set; }

    [JsonPropertyName("comparison")]
    public QueryComparison? Comparison { get; set; }

    [JsonPropertyName("confidence")]
    public double Confidence { get; set; } = 0.5;

    [JsonPropertyName("explanation")]
    public string? Explanation { get; set; }

    [JsonPropertyName("group_by")]
    public string? GroupBy { get; set; }

    [JsonPropertyName("period")]
    public string? Period { get; set; }

    [JsonPropertyName("periods")]
    public List<string>? Periods { get; set; }

    [JsonPropertyName("visualization_hint")]
    public string? VisualizationHint { get; set; }

    [JsonExtensionData]
    public Dictionary<string, JsonElement>? ExtraFields { get; set; }
}

public static class QueryIntentKinds
{
    public const string Query = "query";
    public const string Chat = "chat";
    public const string Clarify = "clarify";
    public const string MetricQuery = "metric_query";
    public const string ComparePeriods = "compare_periods";
}

public sealed class IntentFilter
{
    [JsonPropertyName("field")]
    public string? Field { get; set; }

    [JsonPropertyName("operator")]
    public string? Operator { get; set; } = "=";

    [JsonPropertyName("value")]
    public object? Value { get; set; }

    [JsonExtensionData]
    public Dictionary<string, JsonElement>? ExtraFields { get; set; }
}

public sealed class QueryDateRange
{
    [JsonPropertyName("type")]
    public string? Type { get; set; }

    [JsonPropertyName("value")]
    public int? Value { get; set; }

    [JsonPropertyName("unit")]
    public string? Unit { get; set; }

    [JsonPropertyName("start")]
    public string? Start { get; set; }

    [JsonPropertyName("end")]
    public string? End { get; set; }

    [JsonPropertyName("date_column")]
    public string? DateColumn { get; set; }

    [JsonPropertyName("label")]
    public string? Label { get; set; }

    [JsonExtensionData]
    public Dictionary<string, JsonElement>? ExtraFields { get; set; }
}

public sealed class QuerySort
{
    [JsonPropertyName("field")]
    public string? Field { get; set; }

    [JsonPropertyName("direction")]
    public string? Direction { get; set; } = "asc";

    [JsonExtensionData]
    public Dictionary<string, JsonElement>? ExtraFields { get; set; }
}

public sealed class QueryComparison
{
    [JsonPropertyName("mode")]
    public string? Mode { get; set; }

    [JsonPropertyName("periods")]
    public List<QueryDateRange> Periods { get; set; } = new();

    [JsonExtensionData]
    public Dictionary<string, JsonElement>? ExtraFields { get; set; }
}

public static class IntentValueHelper
{
    public static IReadOnlyList<object?> ToValues(object? value)
    {
        if (value is null)
            return Array.Empty<object?>();

        if (value is string)
            return new[] { value };

        if (value is JsonElement element)
            return ExtractFromJson(element);

        if (value is IEnumerable<object?> objects)
            return objects.ToList();

        if (value is IEnumerable<string> strings)
            return strings.Cast<object?>().ToList();

        return new[] { value };
    }

    public static string? ToDisplayString(object? value)
    {
        if (value is null)
            return null;

        if (value is JsonElement element)
        {
            return element.ValueKind switch
            {
                JsonValueKind.String => element.GetString(),
                JsonValueKind.Number => element.GetRawText(),
                JsonValueKind.True => "true",
                JsonValueKind.False => "false",
                JsonValueKind.Array => string.Join(", ", ExtractFromJson(element).Select(ToDisplayString)),
                JsonValueKind.Object => element.GetRawText(),
                _ => null
            };
        }

        if (value is IEnumerable<object?> objects && value is not string)
            return string.Join(", ", objects.Select(ToDisplayString));

        if (value is IEnumerable<string> strings && value is not string)
            return string.Join(", ", strings);

        return Convert.ToString(value, CultureInfo.InvariantCulture);
    }

    public static object? NormalizeScalar(object? value)
    {
        if (value is JsonElement element)
        {
            return element.ValueKind switch
            {
                JsonValueKind.String => element.GetString(),
                JsonValueKind.Number when element.TryGetInt64(out var longValue) => longValue,
                JsonValueKind.Number when element.TryGetDouble(out var doubleValue) => doubleValue,
                JsonValueKind.True => true,
                JsonValueKind.False => false,
                JsonValueKind.Null => null,
                _ => element.GetRawText()
            };
        }

        return value;
    }

    private static IReadOnlyList<object?> ExtractFromJson(JsonElement element)
    {
        if (element.ValueKind != JsonValueKind.Array)
            return new[] { NormalizeScalar(element) };

        var values = new List<object?>();
        foreach (var item in element.EnumerateArray())
            values.Add(NormalizeScalar(item.Clone()));

        return values;
    }
}

public sealed class IntentFilterListJsonConverter : JsonConverter<List<IntentFilter>>
{
    public override List<IntentFilter> Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        if (reader.TokenType == JsonTokenType.Null)
            return new List<IntentFilter>();

        using var document = JsonDocument.ParseValue(ref reader);
        var root = document.RootElement;

        if (root.ValueKind == JsonValueKind.Array)
        {
            var filters = new List<IntentFilter>();
            foreach (var item in root.EnumerateArray())
            {
                if (item.ValueKind != JsonValueKind.Object)
                    continue;

                var filter = item.Deserialize<IntentFilter>(options);
                if (filter != null)
                    filters.Add(filter);
            }

            return filters;
        }

        if (root.ValueKind == JsonValueKind.Object)
        {
            var filters = new List<IntentFilter>();
            foreach (var property in root.EnumerateObject())
            {
                filters.Add(new IntentFilter
                {
                    Field = property.Name,
                    Operator = "=",
                    Value = property.Value.Clone()
                });
            }

            return filters;
        }

        return new List<IntentFilter>();
    }

    public override void Write(Utf8JsonWriter writer, List<IntentFilter> value, JsonSerializerOptions options)
    {
        writer.WriteStartArray();
        foreach (var item in value)
            JsonSerializer.Serialize(writer, item, options);
        writer.WriteEndArray();
    }
}
