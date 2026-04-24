using System.Globalization;
using System.Text.RegularExpressions;

namespace DriveeDataSpace.Core.Services;

public record PeriodRange(DateTime From, DateTime To, string Label);

public static class PeriodResolver
{
    public static PeriodRange? Resolve(string? period, DateTime? now = null)
    {
        if (string.IsNullOrWhiteSpace(period))
            return null;

        var anchor = (now ?? DateTime.UtcNow).Date;
        var key = period.Trim().ToLowerInvariant();

        if (TryResolveRolling(key, anchor, out var rolling))
            return rolling;

        return key switch
        {
            "today" => new PeriodRange(anchor, anchor.AddDays(1), "сегодня"),
            "yesterday" => new PeriodRange(anchor.AddDays(-1), anchor, "вчера"),
            "current_week" or "this_week" => ResolveCurrentWeek(anchor),
            "last_week" or "previous_week" => ResolvePreviousWeek(anchor),
            "current_month" or "this_month" => new PeriodRange(
                new DateTime(anchor.Year, anchor.Month, 1),
                new DateTime(anchor.Year, anchor.Month, 1).AddMonths(1),
                "текущий месяц"),
            "last_month" or "previous_month" => new PeriodRange(
                new DateTime(anchor.Year, anchor.Month, 1).AddMonths(-1),
                new DateTime(anchor.Year, anchor.Month, 1),
                "прошлый месяц"),
            "current_year" or "this_year" => new PeriodRange(
                new DateTime(anchor.Year, 1, 1),
                new DateTime(anchor.Year + 1, 1, 1),
                "текущий год"),
            "previous_year" or "last_year" => new PeriodRange(
                new DateTime(anchor.Year - 1, 1, 1),
                new DateTime(anchor.Year, 1, 1),
                "прошлый год"),
            _ => TryResolveAbsolute(key, out var absolute) ? absolute : null
        };
    }

    private static bool TryResolveRolling(string key, DateTime anchor, out PeriodRange? range)
    {
        var daysMatch = Regex.Match(key, @"^last_(\d+)_days$", RegexOptions.CultureInvariant);
        if (daysMatch.Success && int.TryParse(daysMatch.Groups[1].Value, out var days) && days > 0)
        {
            range = new PeriodRange(anchor.AddDays(-(days - 1)), anchor.AddDays(1), $"последние {days} {FormatRussianDays(days)}");
            return true;
        }

        var weeksMatch = Regex.Match(key, @"^last_(\d+)_weeks$", RegexOptions.CultureInvariant);
        if (weeksMatch.Success && int.TryParse(weeksMatch.Groups[1].Value, out var weeks) && weeks > 0)
        {
            range = new PeriodRange(anchor.AddDays(-(weeks * 7 - 1)), anchor.AddDays(1), $"последние {weeks} {FormatRussianWeeks(weeks)}");
            return true;
        }

        var monthsMatch = Regex.Match(key, @"^last_(\d+)_months$", RegexOptions.CultureInvariant);
        if (monthsMatch.Success && int.TryParse(monthsMatch.Groups[1].Value, out var months) && months > 0)
        {
            var start = new DateTime(anchor.Year, anchor.Month, 1).AddMonths(-(months - 1));
            var end = new DateTime(anchor.Year, anchor.Month, 1).AddMonths(1);
            range = new PeriodRange(start, end, $"последние {months} {FormatRussianMonths(months)}");
            return true;
        }

        range = null;
        return false;
    }

    private static bool TryResolveAbsolute(string key, out PeriodRange? range)
    {
        var match = Regex.Match(
            key,
            @"^(?<from>\d{4}-\d{2}-\d{2}|\d{2}\.\d{2}\.\d{4})\s*(?:\.\.|-|to|по|до)\s*(?<to>\d{4}-\d{2}-\d{2}|\d{2}\.\d{2}\.\d{4})$",
            RegexOptions.CultureInvariant);

        if (match.Success &&
            TryParseDate(match.Groups["from"].Value, out var from) &&
            TryParseDate(match.Groups["to"].Value, out var to) &&
            from <= to)
        {
            range = new PeriodRange(from, to.AddDays(1), $"с {from:dd.MM.yyyy} по {to:dd.MM.yyyy}");
            return true;
        }

        range = null;
        return false;
    }

    private static bool TryParseDate(string text, out DateTime date)
    {
        var formats = new[] { "yyyy-MM-dd", "dd.MM.yyyy", "yyyy/MM/dd" };
        return DateTime.TryParseExact(
            text,
            formats,
            CultureInfo.InvariantCulture,
            DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal,
            out date);
    }

    private static PeriodRange ResolveCurrentWeek(DateTime anchor)
    {
        var dayOfWeek = (int)anchor.DayOfWeek;
        var monday = anchor.AddDays(-(dayOfWeek == 0 ? 6 : dayOfWeek - 1));
        return new PeriodRange(monday, monday.AddDays(7), "текущая неделя");
    }

    private static PeriodRange ResolvePreviousWeek(DateTime anchor)
    {
        var currentWeek = ResolveCurrentWeek(anchor);
        return new PeriodRange(currentWeek.From.AddDays(-7), currentWeek.From, "прошлая неделя");
    }

    private static string FormatRussianDays(int value)
    {
        var mod100 = value % 100;
        if (mod100 is >= 11 and <= 14)
            return "дней";

        return (value % 10) switch
        {
            1 => "день",
            2 or 3 or 4 => "дня",
            _ => "дней"
        };
    }

    private static string FormatRussianWeeks(int value)
    {
        var mod100 = value % 100;
        if (mod100 is >= 11 and <= 14)
            return "недель";

        return (value % 10) switch
        {
            1 => "неделя",
            2 or 3 or 4 => "недели",
            _ => "недель"
        };
    }

    private static string FormatRussianMonths(int value)
    {
        var mod100 = value % 100;
        if (mod100 is >= 11 and <= 14)
            return "месяцев";

        return (value % 10) switch
        {
            1 => "месяц",
            2 or 3 or 4 => "месяца",
            _ => "месяцев"
        };
    }
}
