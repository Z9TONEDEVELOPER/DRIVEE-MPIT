using NexusDataSpace.Core.Models;

namespace NexusDataSpace.Core.Services;

public sealed record ExplainResult(
    string Summary,
    string Technical,
    QueryExplanation Structured,
    List<ReasoningStep> Trail);

public class ExplainEngine
{
    public ExplainResult Build(ValidatedIntent intent, BuiltSql builtSql, string userQuery, bool isReplay = false)
    {
        var structured = new QueryExplanation
        {
            MetricLabel = intent.Metric.DisplayLabel,
            DimensionsLabel = intent.Dimensions.Count == 0
                ? "без группировки"
                : string.Join(", ", intent.Dimensions.Select(dimension => dimension.Definition.DisplayLabel)),
            PeriodLabel = intent.ComparisonRanges.Count > 0
                ? string.Join(" vs ", intent.ComparisonRanges.Select(range => range.Label))
                : intent.DateRange?.Label ?? "за весь доступный период",
            FiltersLabel = intent.Filters.Select(filter => filter.Label).ToList(),
            AggregationLabel = intent.Aggregation switch
            {
                "count" => "количество",
                "sum" => "сумма",
                "avg" => "среднее",
                "formula" => "вычисляемая метрика",
                _ => intent.Aggregation
            },
            VisualizationLabel = intent.Visualization switch
            {
                "line" => "линейный график",
                "bar" => "столбчатая диаграмма",
                "pie" => "круговая диаграмма",
                _ => "таблица"
            },
            SortLabel = intent.Sort.Count == 0
                ? "без явной сортировки"
                : string.Join(", ", intent.Sort.Select(sort => sort.Label)),
            SourceLabel = $"{intent.Source.DisplayName} (`{intent.Source.Table}`)",
            Limit = intent.Limit,
            Confidence = intent.NormalizedIntent.Confidence
        };

        var summaryParts = new List<string> { structured.MetricLabel };
        if (intent.Dimensions.Count > 0)
            summaryParts.Add(structured.DimensionsLabel);
        summaryParts.Add(structured.PeriodLabel);
        if (structured.FiltersLabel.Count > 0)
            summaryParts.Add($"фильтры: {string.Join(", ", structured.FiltersLabel)}");
        if (isReplay)
            summaryParts.Add("повторный запуск сохранённого отчёта");

        var trail = new List<ReasoningStep>();
        if (!string.IsNullOrWhiteSpace(userQuery))
        {
            trail.Add(new ReasoningStep(
                "Q",
                "Запрос пользователя",
                $"«{userQuery}»"));
        }

        trail.Add(new ReasoningStep(
            "1",
            "Метрика",
            $"Распознана метрика «{intent.Metric.DisplayLabel}» (`{intent.Metric.Key}`) с агрегацией `{intent.Aggregation}`."));

        trail.Add(new ReasoningStep(
            "2",
            "Источник данных",
            $"Используется источник `{intent.Source.Key}` и таблица `{intent.Source.Table}`."));

        if (intent.ComparisonRanges.Count > 0)
        {
            trail.Add(new ReasoningStep(
                "3",
                "Сравнение периодов",
                string.Join(Environment.NewLine, intent.ComparisonRanges.Select((range, index) =>
                    $"{index + 1}. {range.Label}: {range.FromUtc:yyyy-MM-dd} .. {range.ToExclusiveUtc.AddDays(-1):yyyy-MM-dd}" +
                    (range.AnchoredToLatestData ? $" (якорь = последняя дата данных {range.AnchorDateUtc:yyyy-MM-dd})" : string.Empty)))));
        }
        else if (intent.DateRange != null)
        {
            trail.Add(new ReasoningStep(
                "3",
                "Период",
                $"{intent.DateRange.Label}: {intent.DateRange.FromUtc:yyyy-MM-dd} .. {intent.DateRange.ToExclusiveUtc.AddDays(-1):yyyy-MM-dd}" +
                (intent.DateRange.AnchoredToLatestData ? $" (относительная дата посчитана от последней даты данных {intent.DateRange.AnchorDateUtc:yyyy-MM-dd})" : string.Empty)));
        }

        if (intent.Filters.Count > 0)
        {
            trail.Add(new ReasoningStep(
                "4",
                "Фильтры",
                string.Join(Environment.NewLine, intent.Filters.Select(filter => filter.Label))));
        }

        if (intent.Dimensions.Count > 0)
        {
            trail.Add(new ReasoningStep(
                "5",
                "Группировка",
                string.Join(Environment.NewLine, intent.Dimensions.Select(dimension =>
                    $"{dimension.Definition.DisplayLabel}: `{dimension.SqlExpression}`"))));
        }

        if (intent.Sort.Count > 0 || intent.Limit > 0)
        {
            trail.Add(new ReasoningStep(
                "6",
                "Сортировка и лимит",
                $"Сортировка: {structured.SortLabel}. LIMIT = {intent.Limit}."));
        }

        trail.Add(new ReasoningStep(
            "7",
            "Guardrails",
            "Финальный SQL строится только кодом, разрешён только SELECT, запрещён SELECT *, применяются белые списки таблиц и полей, а LIMIT обязателен."));

        var technical = string.Join(
            " | ",
            $"intent={intent.NormalizedIntent.Intent}",
            $"metric={intent.Metric.Key}",
            $"aggregation={intent.Aggregation}",
            $"source={intent.Source.Key}",
            $"dimensions={string.Join(",", intent.Dimensions.Select(dimension => dimension.Definition.Key))}",
            $"filters={string.Join(",", intent.Filters.Select(filter => $"{filter.Definition.Key}:{filter.Operator}:{string.Join(",", filter.Values.Select(IntentValueHelper.ToDisplayString))}"))}",
            $"period={structured.PeriodLabel}",
            $"sort={string.Join(",", intent.Sort.Select(sort => $"{sort.FieldKey}:{sort.Direction}"))}",
            $"limit={intent.Limit}",
            $"signature={builtSql.Signature}");

        return new ExplainResult(
            string.Join(", ", summaryParts) + ".",
            technical,
            structured,
            trail);
    }
}
