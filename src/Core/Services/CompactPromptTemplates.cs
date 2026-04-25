using System.Text;
using DriveeDataSpace.Core.Models;

namespace DriveeDataSpace.Core.Services;

internal static class CompactPromptTemplates
{
    public static string SystemPrompt(SemanticLayer semanticLayer)
    {
        var metrics = string.Join(Environment.NewLine, semanticLayer.Metrics.Select(metric =>
            $"- {metric.Key}: agg={metric.Aggregation}; source={metric.Source}; date={metric.DateColumn}; dims={JoinKeys(metric.AllowedDimensions, 10)}; filters={JoinKeys(metric.AllowedFilters, 10)}"));
        var dimensions = string.Join(Environment.NewLine, semanticLayer.Dimensions.Select(dimension =>
            $"- {dimension.Key}: source={dimension.Source}; time={dimension.IsTimeDimension}"));
        var filters = string.Join(Environment.NewLine, semanticLayer.Filters.Select(filter =>
            $"- {filter.Key}: source={filter.Source}; ops={JoinKeys(filter.AllowedOperators, 8)}"));
        var sources = string.Join(Environment.NewLine, semanticLayer.Sources.Select(source =>
            $"- {source.Key}: table={source.Table}; columns={JoinKeys(source.AllowedColumns, 16)}"));
        var presets = semanticLayer.Presets.Count == 0
            ? "- none"
            : string.Join(Environment.NewLine, semanticLayer.Presets.Take(8).Select(preset =>
                $"- phrases={JoinKeys(preset.Phrases, 3)}; metric={preset.Metric ?? ""}; dimension={preset.Dimension ?? ""}; filters={JoinKeys(preset.Filters.Select(RenderFilter), 4)}; visualization={preset.Visualization ?? ""}"));

        var builder = new StringBuilder();
        builder.AppendLine("You are a fast NL-to-BI-intent parser. Return ONLY one compact JSON object. No markdown. No SQL.");
        builder.AppendLine("Use only keys listed below. If the request is not analytical, return kind=chat. If key data is missing, return kind=clarify.");
        builder.AppendLine("For time series use visualization=line. For top N use limit and sort by metric desc. Prefer clarify for ambiguous 'last week/month/year'.");
        builder.AppendLine();
        builder.AppendLine("Metrics:");
        builder.AppendLine(metrics);
        builder.AppendLine("Dimensions:");
        builder.AppendLine(dimensions);
        builder.AppendLine("Filters:");
        builder.AppendLine(filters);
        builder.AppendLine("Sources:");
        builder.AppendLine(sources);
        builder.AppendLine("Presets:");
        builder.AppendLine(presets);
        builder.AppendLine();
        builder.AppendLine($"Allowed dimension keys: {JoinKeys(semanticLayer.Dimensions.Select(dimension => dimension.Key), 40)}");
        builder.AppendLine($"Allowed filter keys: {JoinKeys(semanticLayer.Filters.Select(filter => filter.Key), 40)}");
        builder.AppendLine($"Allowed source keys: {JoinKeys(semanticLayer.Sources.Select(source => source.Key), 20)}");
        builder.AppendLine($"Allowed date columns: {JoinKeys(semanticLayer.Metrics.Select(metric => metric.DateColumn).Distinct(StringComparer.OrdinalIgnoreCase), 20)}");
        builder.AppendLine("JSON shape:");
        builder.AppendLine("{\"kind\":\"query|chat|clarify\",\"reply\":null,\"clarification\":null,\"intent\":\"metric_query|compare_periods\",\"metric\":null,\"aggregation\":null,\"dimensions\":[],\"filters\":[],\"date_range\":null,\"sort\":[],\"limit\":null,\"source\":null,\"comparison\":null,\"confidence\":0.0,\"explanation\":null}");
        builder.AppendLine("filter={\"field\":\"filter_key\",\"operator\":\"=|!=|in|not_in|>|>=|<|<=\",\"value\":\"...\"}");
        builder.AppendLine("date_range.type=today|yesterday|last_n_days|last_n_weeks|last_n_months|current_week|previous_week|current_month|previous_month|current_year|previous_year|absolute.");
        return builder.ToString();
    }

    private static string JoinKeys(IEnumerable<string> values, int max) =>
        string.Join(",", values.Where(value => !string.IsNullOrWhiteSpace(value)).Take(max));

    private static string RenderFilter(IntentFilter filter) =>
        $"{filter.Field} {filter.Operator ?? "="} {IntentValueHelper.ToDisplayString(filter.Value)}";
}
