using System.Text.RegularExpressions;

namespace DriveeDataSpace.Web.Services;

public record PeriodRange(DateTime From, DateTime To, string Label);

public static class PeriodResolver
{
    public static PeriodRange? Resolve(string? period, DateTime? now = null)
    {
        if (string.IsNullOrWhiteSpace(period)) return null;

        var today = (now ?? DateTime.UtcNow).Date;
        var key = period.Trim().ToLowerInvariant();

        if (TryResolveRollingDays(key, today, out var rolling))
            return rolling;

        return key switch
        {
            "today" => new(today, today.AddDays(1), "\u0441\u0435\u0433\u043e\u0434\u043d\u044f"),
            "yesterday" => new(today.AddDays(-1), today, "\u0432\u0447\u0435\u0440\u0430"),
            "last_week" => LastWeek(today),
            "current_week" => CurrentWeek(today),
            "last_month" => new(
                new DateTime(today.Year, today.Month, 1).AddMonths(-1),
                new DateTime(today.Year, today.Month, 1),
                "\u043f\u0440\u043e\u0448\u043b\u044b\u0439 \u043c\u0435\u0441\u044f\u0446"),
            "current_month" => new(
                new DateTime(today.Year, today.Month, 1),
                new DateTime(today.Year, today.Month, 1).AddMonths(1),
                "\u0442\u0435\u043a\u0443\u0449\u0438\u0439 \u043c\u0435\u0441\u044f\u0446"),
            "current_year" => new(
                new DateTime(today.Year, 1, 1),
                new DateTime(today.Year + 1, 1, 1),
                "\u0442\u0435\u043a\u0443\u0449\u0438\u0439 \u0433\u043e\u0434"),
            "previous_year" => new(
                new DateTime(today.Year - 1, 1, 1),
                new DateTime(today.Year, 1, 1),
                "\u043f\u0440\u043e\u0448\u043b\u044b\u0439 \u0433\u043e\u0434"),
            _ => null
        };
    }

    private static bool TryResolveRollingDays(string period, DateTime today, out PeriodRange? range)
    {
        var match = Regex.Match(period, @"^last_(\d+)_days$", RegexOptions.CultureInvariant);
        if (!match.Success || !int.TryParse(match.Groups[1].Value, out var days) || days <= 0)
        {
            range = null;
            return false;
        }

        var from = today.AddDays(-(days - 1));
        range = new(from, today.AddDays(1), $"\u043f\u043e\u0441\u043b\u0435\u0434\u043d\u0438\u0435 {days} {FormatRussianDays(days)}");
        return true;
    }

    private static string FormatRussianDays(int days)
    {
        var mod100 = days % 100;
        if (mod100 is >= 11 and <= 14) return "\u0434\u043d\u0435\u0439";

        return (days % 10) switch
        {
            1 => "\u0434\u0435\u043d\u044c",
            2 or 3 or 4 => "\u0434\u043d\u044f",
            _ => "\u0434\u043d\u0435\u0439"
        };
    }

    private static PeriodRange CurrentWeek(DateTime today)
    {
        var dow = (int)today.DayOfWeek;
        var monday = today.AddDays(-(dow == 0 ? 6 : dow - 1));
        return new(monday, monday.AddDays(7), "\u0442\u0435\u043a\u0443\u0449\u0430\u044f \u043d\u0435\u0434\u0435\u043b\u044f");
    }

    private static PeriodRange LastWeek(DateTime today)
    {
        var cur = CurrentWeek(today);
        return new(cur.From.AddDays(-7), cur.From, "\u043f\u0440\u043e\u0448\u043b\u0430\u044f \u043d\u0435\u0434\u0435\u043b\u044f");
    }
}
