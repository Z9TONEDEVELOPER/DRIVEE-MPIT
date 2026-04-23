namespace DriveeDataSpace.Web.Services;

public record PeriodRange(DateTime From, DateTime To, string Label);

public static class PeriodResolver
{
    public static PeriodRange? Resolve(string? period, DateTime? now = null)
    {
        if (string.IsNullOrWhiteSpace(period)) return null;
        var n = (now ?? DateTime.UtcNow).Date;
        return period.Trim().ToLowerInvariant() switch
        {
            "today"           => new(n, n.AddDays(1), "сегодня"),
            "yesterday"       => new(n.AddDays(-1), n, "вчера"),
            "last_7_days"     => new(n.AddDays(-7), n.AddDays(1), "последние 7 дней"),
            "last_30_days"    => new(n.AddDays(-30), n.AddDays(1), "последние 30 дней"),
            "last_week"       => LastWeek(n),
            "current_week"    => CurrentWeek(n),
            "last_month"      => new(new DateTime(n.Year, n.Month, 1).AddMonths(-1), new DateTime(n.Year, n.Month, 1), "прошлый месяц"),
            "current_month"   => new(new DateTime(n.Year, n.Month, 1), new DateTime(n.Year, n.Month, 1).AddMonths(1), "текущий месяц"),
            "current_year"    => new(new DateTime(n.Year, 1, 1), new DateTime(n.Year + 1, 1, 1), "текущий год"),
            "previous_year"   => new(new DateTime(n.Year - 1, 1, 1), new DateTime(n.Year, 1, 1), "прошлый год"),
            _ => null
        };
    }

    private static PeriodRange CurrentWeek(DateTime n)
    {
        var dow = (int)n.DayOfWeek;
        var monday = n.AddDays(-(dow == 0 ? 6 : dow - 1));
        return new(monday, monday.AddDays(7), "текущая неделя");
    }

    private static PeriodRange LastWeek(DateTime n)
    {
        var cur = CurrentWeek(n);
        return new(cur.From.AddDays(-7), cur.From, "прошлая неделя");
    }
}
