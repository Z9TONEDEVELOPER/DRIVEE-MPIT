using System.Text.Json;

namespace NexusDataSpace.Core.Services;

internal static class IntentJsonSchema
{
    public const string SchemaName = "query_intent";

    public static object Build(SemanticLayer semanticLayer)
    {
        var dimensionKeys = ToEnum(semanticLayer.Dimensions.Select(dimension => dimension.Key));
        var filterFieldKeys = ToEnum(semanticLayer.Filters.Select(filter => filter.Key));
        var sourceKeys = ToEnum(semanticLayer.Sources.Select(source => source.Key));
        var metricKeys = ToEnum(semanticLayer.Metrics.Select(metric => metric.Key));
        var dateColumns = ToEnum(semanticLayer.Metrics
            .Select(metric => metric.DateColumn)
            .Where(column => !string.IsNullOrWhiteSpace(column))
            .Distinct(StringComparer.OrdinalIgnoreCase));

        var sortFieldKeys = ToEnum(new[] { "metric", "period" }
            .Concat(semanticLayer.Dimensions.Select(dimension => dimension.Key))
            .Distinct(StringComparer.OrdinalIgnoreCase));

        var nullableString = new { type = new[] { "string", "null" } };
        var nullableInteger = new { type = new[] { "integer", "null" } };

        var dateRangeSchema = new
        {
            type = new[] { "object", "null" },
            additionalProperties = false,
            properties = new Dictionary<string, object>
            {
                ["type"] = new
                {
                    type = new[] { "string", "null" },
                    @enum = new[]
                    {
                        "today", "yesterday",
                        "last_n_days", "last_n_weeks", "last_n_months",
                        "current_week", "previous_week",
                        "current_month", "previous_month",
                        "current_year", "previous_year",
                        "absolute",
                        null!
                    }
                },
                ["value"] = nullableInteger,
                ["unit"] = nullableString,
                ["start"] = nullableString,
                ["end"] = nullableString,
                ["date_column"] = dateColumns.Count == 0
                    ? (object)nullableString
                    : new { type = new[] { "string", "null" }, @enum = WithNull(dateColumns) },
                ["label"] = nullableString
            },
            required = new[] { "type", "value", "unit", "start", "end", "date_column", "label" }
        };

        var filterItemSchema = new
        {
            type = "object",
            additionalProperties = false,
            properties = new Dictionary<string, object>
            {
                ["field"] = filterFieldKeys.Count == 0
                    ? (object)new { type = "string" }
                    : new { type = "string", @enum = filterFieldKeys },
                ["operator"] = new
                {
                    type = "string",
                    @enum = new[] { "=", "!=", "in", "not_in", ">", ">=", "<", "<=" }
                },
                ["value"] = new
                {
                    type = new[] { "string", "number", "boolean", "array", "null" }
                }
            },
            required = new[] { "field", "operator", "value" }
        };

        var sortItemSchema = new
        {
            type = "object",
            additionalProperties = false,
            properties = new Dictionary<string, object>
            {
                ["field"] = sortFieldKeys.Count == 0
                    ? (object)new { type = "string" }
                    : new { type = "string", @enum = sortFieldKeys },
                ["direction"] = new
                {
                    type = "string",
                    @enum = new[] { "asc", "desc" }
                }
            },
            required = new[] { "field", "direction" }
        };

        var comparisonSchema = new
        {
            type = new[] { "object", "null" },
            additionalProperties = false,
            properties = new Dictionary<string, object>
            {
                ["mode"] = new
                {
                    type = new[] { "string", "null" },
                    @enum = new[] { "period_vs_period", null! }
                },
                ["periods"] = new
                {
                    type = "array",
                    items = dateRangeSchema
                }
            },
            required = new[] { "mode", "periods" }
        };

        var rootProperties = new Dictionary<string, object>
        {
            ["kind"] = new
            {
                type = "string",
                @enum = new[] { "query", "chat", "clarify" }
            },
            ["reply"] = nullableString,
            ["clarification"] = nullableString,
            ["intent"] = new
            {
                type = "string",
                @enum = new[] { "metric_query", "compare_periods" }
            },
            ["metric"] = metricKeys.Count == 0
                ? (object)nullableString
                : new { type = new[] { "string", "null" }, @enum = WithNull(metricKeys) },
            ["aggregation"] = new
            {
                type = new[] { "string", "null" },
                @enum = new[] { "count", "sum", "avg", "max", "formula", null! }
            },
            ["dimensions"] = new
            {
                type = "array",
                items = dimensionKeys.Count == 0
                    ? (object)new { type = "string" }
                    : new { type = "string", @enum = dimensionKeys }
            },
            ["filters"] = new
            {
                type = "array",
                items = filterItemSchema
            },
            ["date_range"] = dateRangeSchema,
            ["sort"] = new
            {
                type = "array",
                items = sortItemSchema
            },
            ["limit"] = nullableInteger,
            ["visualization"] = new
            {
                type = new[] { "string", "null" },
                @enum = new[] { "table", "bar", "line", "pie", null! }
            },
            ["source"] = sourceKeys.Count == 0
                ? (object)nullableString
                : new { type = new[] { "string", "null" }, @enum = WithNull(sourceKeys) },
            ["comparison"] = comparisonSchema,
            ["confidence"] = new
            {
                type = "number",
                minimum = 0.0,
                maximum = 1.0
            },
            ["explanation"] = nullableString
        };

        var rootRequired = new[]
        {
            "kind", "reply", "clarification", "intent", "metric", "aggregation",
            "dimensions", "filters", "date_range", "sort", "limit",
            "visualization", "source", "comparison", "confidence", "explanation"
        };

        return new
        {
            type = "object",
            additionalProperties = false,
            properties = rootProperties,
            required = rootRequired
        };
    }

    public static string Serialize(SemanticLayer semanticLayer) =>
        JsonSerializer.Serialize(Build(semanticLayer));

    private static List<string> ToEnum(IEnumerable<string> values) =>
        values
            .Where(value => !string.IsNullOrWhiteSpace(value))
            .Select(value => value.Trim())
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();

    private static string?[] WithNull(IEnumerable<string> values)
    {
        var list = values.Cast<string?>().ToList();
        list.Add(null);
        return list.ToArray();
    }
}
