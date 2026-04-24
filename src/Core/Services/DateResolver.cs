using System.Globalization;
using System.Text.RegularExpressions;
using DriveeDataSpace.Core.Models;

namespace DriveeDataSpace.Core.Services;

public sealed record ResolvedDateRange(
    DateTime FromUtc,
    DateTime ToExclusiveUtc,
    string Label,
    string DateColumn,
    DateTime AnchorDateUtc,
    bool AnchoredToLatestData);

public class DateResolver
{
    private static readonly Regex AmbiguousWeekPeriodPattern = new(
        @"\b(?:за\s+)?последн\w+\s+недел\w+\b",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

    private static readonly Regex AmbiguousMonthPeriodPattern = new(
        @"\b(?:за\s+)?последн\w+\s+месяц\w*\b",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

    private static readonly Regex AmbiguousYearPeriodPattern = new(
        @"\b(?:за\s+)?последн\w+\s+год\w*\b",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

    private static readonly Regex UnresolvedTemporalHintPattern = new(
        @"\b(?:за|for|during|over)\s+(?:(?:последн\w+|прошл\w+|текущ\w+|эту|этот|эти|last|previous|current|this)\s+)?(?:\d+\s*)?(?:дн\w*|недел\w*|месяц\w*|год\w*|day|days|week|weeks|month|months|year|years)\b",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

    private readonly AnalyticsTimeService _analyticsTimeService;

    public DateResolver(AnalyticsTimeService analyticsTimeService)
    {
        _analyticsTimeService = analyticsTimeService;
    }

    public QueryDateRange? NormalizeLegacy(string? legacyPeriod, string defaultDateColumn)
    {
        if (string.IsNullOrWhiteSpace(legacyPeriod))
            return null;

        var key = legacyPeriod.Trim().ToLowerInvariant();
        if (Regex.IsMatch(key, @"^last_\d+_days$", RegexOptions.CultureInvariant))
        {
            var match = Regex.Match(key, @"^last_(\d+)_days$", RegexOptions.CultureInvariant);
            return new QueryDateRange
            {
                Type = "last_n_days",
                Value = int.Parse(match.Groups[1].Value, CultureInfo.InvariantCulture),
                DateColumn = defaultDateColumn
            };
        }

        if (Regex.IsMatch(key, @"^last_\d+_weeks$", RegexOptions.CultureInvariant))
        {
            var match = Regex.Match(key, @"^last_(\d+)_weeks$", RegexOptions.CultureInvariant);
            return new QueryDateRange
            {
                Type = "last_n_weeks",
                Value = int.Parse(match.Groups[1].Value, CultureInfo.InvariantCulture),
                DateColumn = defaultDateColumn
            };
        }

        if (Regex.IsMatch(key, @"^last_\d+_months$", RegexOptions.CultureInvariant))
        {
            var match = Regex.Match(key, @"^last_(\d+)_months$", RegexOptions.CultureInvariant);
            return new QueryDateRange
            {
                Type = "last_n_months",
                Value = int.Parse(match.Groups[1].Value, CultureInfo.InvariantCulture),
                DateColumn = defaultDateColumn
            };
        }

        return key switch
        {
            "today" => new QueryDateRange { Type = "today", DateColumn = defaultDateColumn },
            "yesterday" => new QueryDateRange { Type = "yesterday", DateColumn = defaultDateColumn },
            "current_week" or "this_week" => new QueryDateRange { Type = "current_week", DateColumn = defaultDateColumn },
            "last_week" or "previous_week" => new QueryDateRange { Type = "previous_week", DateColumn = defaultDateColumn },
            "current_month" or "this_month" => new QueryDateRange { Type = "current_month", DateColumn = defaultDateColumn },
            "last_month" or "previous_month" => new QueryDateRange { Type = "previous_month", DateColumn = defaultDateColumn },
            "current_year" or "this_year" => new QueryDateRange { Type = "current_year", DateColumn = defaultDateColumn },
            "previous_year" or "last_year" => new QueryDateRange { Type = "previous_year", DateColumn = defaultDateColumn },
            _ => null
        };
    }

    public ResolvedDateRange? Resolve(QueryDateRange? spec, string defaultDateColumn, string? sourceTable = null, DateTime? nowUtc = null)
    {
        if (spec == null || string.IsNullOrWhiteSpace(spec.Type))
            return null;

        var timeContext = _analyticsTimeService.GetContext(sourceTable, defaultDateColumn);
        var anchor = (nowUtc ?? timeContext.AnchorDateUtc).Date;
        var dateColumn = string.IsNullOrWhiteSpace(spec.DateColumn) ? defaultDateColumn : spec.DateColumn.Trim();
        var type = spec.Type.Trim().ToLowerInvariant();

        if (type == "last_n" && !string.IsNullOrWhiteSpace(spec.Unit))
            type = $"last_n_{spec.Unit.Trim().ToLowerInvariant()}";

        return type switch
        {
            "today" => Create(anchor, anchor.AddDays(1), "сегодня", dateColumn, timeContext),
            "yesterday" => Create(anchor.AddDays(-1), anchor, "вчера", dateColumn, timeContext),
            "last_n_days" => Create(anchor.AddDays(-((spec.Value ?? 1) - 1)), anchor.AddDays(1), BuildRollingLabel(spec.Value ?? 1, "days"), dateColumn, timeContext),
            "last_n_weeks" => Create(anchor.AddDays(-((spec.Value ?? 1) * 7 - 1)), anchor.AddDays(1), BuildRollingLabel(spec.Value ?? 1, "weeks"), dateColumn, timeContext),
            "last_n_months" => Create(new DateTime(anchor.Year, anchor.Month, 1).AddMonths(-((spec.Value ?? 1) - 1)), new DateTime(anchor.Year, anchor.Month, 1).AddMonths(1), BuildRollingLabel(spec.Value ?? 1, "months"), dateColumn, timeContext),
            "current_week" => ResolveCurrentWeek(anchor, dateColumn, timeContext),
            "previous_week" => ResolvePreviousWeek(anchor, dateColumn, timeContext),
            "current_month" => Create(new DateTime(anchor.Year, anchor.Month, 1), new DateTime(anchor.Year, anchor.Month, 1).AddMonths(1), "текущий месяц", dateColumn, timeContext),
            "previous_month" => Create(new DateTime(anchor.Year, anchor.Month, 1).AddMonths(-1), new DateTime(anchor.Year, anchor.Month, 1), "прошлый месяц", dateColumn, timeContext),
            "current_year" => Create(new DateTime(anchor.Year, 1, 1), new DateTime(anchor.Year + 1, 1, 1), "текущий год", dateColumn, timeContext),
            "previous_year" => Create(new DateTime(anchor.Year - 1, 1, 1), new DateTime(anchor.Year, 1, 1), "прошлый год", dateColumn, timeContext),
            "absolute" or "between" => ResolveAbsolute(spec, dateColumn, timeContext),
            _ => null
        };
    }

    public IReadOnlyList<ResolvedDateRange> ResolveComparisonPeriods(QueryIntent intent, string defaultDateColumn, string? sourceTable = null, DateTime? nowUtc = null)
    {
        var specs = new List<QueryDateRange>();

        if (intent.Comparison?.Periods.Count > 0)
            specs.AddRange(intent.Comparison.Periods);

        if (intent.Periods?.Count > 0)
        {
            foreach (var period in intent.Periods)
            {
                var normalized = NormalizeLegacy(period, defaultDateColumn);
                if (normalized != null)
                    specs.Add(normalized);
            }
        }

        return specs
            .Select(spec => Resolve(spec, defaultDateColumn, sourceTable, nowUtc))
            .Where(range => range != null)
            .Cast<ResolvedDateRange>()
            .ToList();
    }

    public bool TryBuildAmbiguousPeriodClarification(string userQuery, out string? clarification)
    {
        clarification = null;
        if (string.IsNullOrWhiteSpace(userQuery))
            return false;

        var text = userQuery.Trim().ToLowerInvariant();

        if (AmbiguousWeekPeriodPattern.IsMatch(text))
        {
            clarification = "Уточните период: вы имеете в виду прошлую календарную неделю или последние 7 дней?";
            return true;
        }

        if (AmbiguousMonthPeriodPattern.IsMatch(text))
        {
            clarification = "Уточните период: вы имеете в виду прошлый календарный месяц или последние 30 дней?";
            return true;
        }

        if (AmbiguousYearPeriodPattern.IsMatch(text))
        {
            clarification = "Уточните период: вы имеете в виду прошлый календарный год или последние 12 месяцев?";
            return true;
        }

        if (UnresolvedTemporalHintPattern.IsMatch(text) && !TryExtractDateRange(userQuery, SemanticLayer.DefaultDateColumn, out _))
        {
            clarification = "Уточните, за какой период нужно показать данные?";
            return true;
        }

        return false;
    }

    public bool TryExtractDateRange(string userQuery, string defaultDateColumn, out QueryDateRange? dateRange)
    {
        dateRange = null;
        if (string.IsNullOrWhiteSpace(userQuery))
            return false;

        var text = userQuery.Trim().ToLowerInvariant();

        var absoluteMatch = Regex.Match(
            text,
            @"(?:с|from)\s+(?<from>\d{4}-\d{2}-\d{2}|\d{2}\.\d{2}\.\d{4})\s+(?:по|to|-)\s+(?<to>\d{4}-\d{2}-\d{2}|\d{2}\.\d{2}\.\d{4})",
            RegexOptions.CultureInvariant);
        if (absoluteMatch.Success)
        {
            dateRange = new QueryDateRange
            {
                Type = "absolute",
                Start = absoluteMatch.Groups["from"].Value,
                End = absoluteMatch.Groups["to"].Value,
                DateColumn = defaultDateColumn
            };
            return true;
        }

        var rolling = Regex.Match(
            text,
            @"(?:за|for|last|последн\w*\s+)?(?<value>\d{1,3})\s*(?<unit>дн(?:ей|я|ь)?|days?|недел(?:ь|и|ю)?|weeks?|месяц(?:а|ев)?|months?)",
            RegexOptions.CultureInvariant);
        if (rolling.Success && int.TryParse(rolling.Groups["value"].Value, out var value) && value > 0)
        {
            var unit = rolling.Groups["unit"].Value;
            dateRange = new QueryDateRange
            {
                Type = unit.Contains("нед", StringComparison.Ordinal) || unit.Contains("week", StringComparison.Ordinal)
                    ? "last_n_weeks"
                    : unit.Contains("меся", StringComparison.Ordinal) || unit.Contains("month", StringComparison.Ordinal)
                        ? "last_n_months"
                        : "last_n_days",
                Value = value,
                DateColumn = defaultDateColumn
            };
            return true;
        }

        var quickMap = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["сегодня"] = "today",
            ["today"] = "today",
            ["вчера"] = "yesterday",
            ["yesterday"] = "yesterday",
            ["эта неделя"] = "current_week",
            ["текущая неделя"] = "current_week",
            ["this week"] = "current_week",
            ["прошлая неделя"] = "previous_week",
            ["last week"] = "previous_week",
            ["этот месяц"] = "current_month",
            ["текущий месяц"] = "current_month",
            ["this month"] = "current_month",
            ["прошлый месяц"] = "previous_month",
            ["last month"] = "previous_month",
            ["этот год"] = "current_year",
            ["текущий год"] = "current_year",
            ["this year"] = "current_year",
            ["прошлый год"] = "previous_year",
            ["last year"] = "previous_year"
        };

        foreach (var pair in quickMap)
        {
            if (!text.Contains(pair.Key, StringComparison.OrdinalIgnoreCase))
                continue;

            dateRange = new QueryDateRange { Type = pair.Value, DateColumn = defaultDateColumn };
            return true;
        }

        return false;
    }

    private static ResolvedDateRange ResolveAbsolute(QueryDateRange spec, string dateColumn, AnalyticsTimeContext timeContext)
    {
        if (!TryParseDate(spec.Start, out var start) || !TryParseDate(spec.End, out var end))
            throw new InvalidOperationException("Некорректный абсолютный период.");

        if (end < start)
            throw new InvalidOperationException("Дата окончания периода раньше даты начала.");

        return Create(start.Date, end.Date.AddDays(1), $"с {start:dd.MM.yyyy} по {end:dd.MM.yyyy}", dateColumn, timeContext);
    }

    private static ResolvedDateRange ResolveCurrentWeek(DateTime anchor, string dateColumn, AnalyticsTimeContext timeContext)
    {
        var dayOfWeek = (int)anchor.DayOfWeek;
        var monday = anchor.AddDays(-(dayOfWeek == 0 ? 6 : dayOfWeek - 1));
        return Create(monday, monday.AddDays(7), "текущая неделя", dateColumn, timeContext);
    }

    private static ResolvedDateRange ResolvePreviousWeek(DateTime anchor, string dateColumn, AnalyticsTimeContext timeContext)
    {
        var currentWeek = ResolveCurrentWeek(anchor, dateColumn, timeContext);
        return Create(currentWeek.FromUtc.AddDays(-7), currentWeek.FromUtc, "прошлая неделя", dateColumn, timeContext);
    }

    private static ResolvedDateRange Create(
        DateTime from,
        DateTime to,
        string label,
        string dateColumn,
        AnalyticsTimeContext timeContext) =>
        new(from, to, label, dateColumn, timeContext.AnchorDateUtc, timeContext.UsesDataAnchor);

    private static bool TryParseDate(string? value, out DateTime date)
    {
        var formats = new[] { "yyyy-MM-dd", "dd.MM.yyyy", "yyyy/MM/dd" };
        return DateTime.TryParseExact(
            value,
            formats,
            CultureInfo.InvariantCulture,
            DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal,
            out date);
    }

    private static string BuildRollingLabel(int value, string unit) => unit switch
    {
        "weeks" => $"последние {value} {FormatRussianUnit(value, "week")}",
        "months" => $"последние {value} {FormatRussianUnit(value, "month")}",
        _ => $"последние {value} {FormatRussianUnit(value, "day")}"
    };

    private static string FormatRussianUnit(int value, string unit)
    {
        var mod100 = value % 100;
        if (mod100 is >= 11 and <= 14)
        {
            return unit switch
            {
                "week" => "недель",
                "month" => "месяцев",
                _ => "дней"
            };
        }

        return unit switch
        {
            "week" => (value % 10) switch
            {
                1 => "неделю",
                2 or 3 or 4 => "недели",
                _ => "недель"
            },
            "month" => (value % 10) switch
            {
                1 => "месяц",
                2 or 3 or 4 => "месяца",
                _ => "месяцев"
            },
            _ => (value % 10) switch
            {
                1 => "день",
                2 or 3 or 4 => "дня",
                _ => "дней"
            }
        };
    }
}
