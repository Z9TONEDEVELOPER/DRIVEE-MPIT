using System.Text.RegularExpressions;

namespace DriveeDataSpace.Core.Services;

internal static class PeriodTextPatterns
{
    private const RegexOptions Options = RegexOptions.IgnoreCase | RegexOptions.CultureInvariant;

    public static bool MentionsCurrentAndPreviousYear(string text) =>
        MentionsBoth(text, CurrentYearPhrases, PreviousYearPhrases) ||
        Regex.IsMatch(text, @"(?:этот|текущий|this|current)\s+и\s+(?:прошлый|предыдущий|last|previous)\s+год", Options) ||
        Regex.IsMatch(text, @"(?:this|current)\s+and\s+(?:last|previous)\s+year", Options);

    public static bool MentionsCurrentAndPreviousMonth(string text) =>
        MentionsBoth(text, CurrentMonthPhrases, PreviousMonthPhrases) ||
        Regex.IsMatch(text, @"(?:этот|текущий|this|current)\s+и\s+(?:прошлый|предыдущий|last|previous)\s+месяц", Options) ||
        Regex.IsMatch(text, @"(?:this|current)\s+and\s+(?:last|previous)\s+month", Options);

    public static bool MentionsCurrentAndPreviousWeek(string text) =>
        MentionsBoth(text, CurrentWeekPhrases, PreviousWeekPhrases) ||
        Regex.IsMatch(text, @"(?:эта|текущая|this|current)\s+и\s+(?:прошлая|предыдущая|last|previous)\s+неделя", Options) ||
        Regex.IsMatch(text, @"(?:this|current)\s+and\s+(?:last|previous)\s+week", Options);

    public static bool TryResolveClarificationPeriod(string answer, string clarification, out string resolvedPeriod)
    {
        resolvedPeriod = "";
        if (string.IsNullOrWhiteSpace(answer) || string.IsNullOrWhiteSpace(clarification))
            return false;

        var normalizedAnswer = NormalizeAnswer(answer);
        var clarificationText = clarification.Trim().ToLowerInvariant();
        if (!LooksLikePeriodClarification(clarificationText))
            return false;

        if (IsRollingPeriodAnswer(normalizedAnswer) || IsAffirmativeAnswer(normalizedAnswer))
            return TryGetRollingPeriod(clarificationText, out resolvedPeriod);

        if (IsCalendarPeriodAnswer(normalizedAnswer))
            return TryGetCalendarPeriod(clarificationText, out resolvedPeriod);

        return false;
    }

    private static bool MentionsBoth(string text, IReadOnlyList<string> left, IReadOnlyList<string> right) =>
        ContainsAny(text, left) && ContainsAny(text, right);

    private static bool ContainsAny(string text, IReadOnlyList<string> phrases) =>
        phrases.Any(phrase => text.Contains(phrase, StringComparison.OrdinalIgnoreCase));

    private static bool LooksLikePeriodClarification(string text) =>
        (text.Contains("уточните период", StringComparison.OrdinalIgnoreCase) ||
         text.Contains("clarify the period", StringComparison.OrdinalIgnoreCase) ||
         text.Contains("which period", StringComparison.OrdinalIgnoreCase)) &&
        (text.Contains("или", StringComparison.OrdinalIgnoreCase) ||
         text.Contains("or", StringComparison.OrdinalIgnoreCase));

    private static bool TryGetRollingPeriod(string text, out string resolvedPeriod)
    {
        if (ContainsAny(text, RollingThirtyDaysPhrases))
        {
            resolvedPeriod = "последние 30 дней";
            return true;
        }

        if (ContainsAny(text, RollingSevenDaysPhrases))
        {
            resolvedPeriod = "последние 7 дней";
            return true;
        }

        if (ContainsAny(text, RollingTwelveMonthsPhrases))
        {
            resolvedPeriod = "последние 12 месяцев";
            return true;
        }

        resolvedPeriod = "";
        return false;
    }

    private static bool TryGetCalendarPeriod(string text, out string resolvedPeriod)
    {
        if (ContainsAny(text, PreviousMonthPhrases) && text.Contains("месяц", StringComparison.OrdinalIgnoreCase))
        {
            resolvedPeriod = "прошлый месяц";
            return true;
        }

        if (ContainsAny(text, PreviousWeekPhrases) && text.Contains("недел", StringComparison.OrdinalIgnoreCase))
        {
            resolvedPeriod = "прошлая неделя";
            return true;
        }

        if (ContainsAny(text, PreviousYearPhrases) && text.Contains("год", StringComparison.OrdinalIgnoreCase))
        {
            resolvedPeriod = "прошлый год";
            return true;
        }

        resolvedPeriod = "";
        return false;
    }

    private static string NormalizeAnswer(string text)
    {
        var normalized = text.Trim().ToLowerInvariant();
        normalized = Regex.Replace(normalized, @"[^\p{L}\p{N}]+", " ", Options);
        return Regex.Replace(normalized, @"\s+", " ", Options).Trim();
    }

    private static bool IsAffirmativeAnswer(string text) =>
        text is "да" or "ага" or "угу" or "ок" or "окей" or "yes" or "yeah" or "yep";

    private static bool IsRollingPeriodAnswer(string text) =>
        text.Contains("последн", StringComparison.OrdinalIgnoreCase) ||
        text.Contains("30 дней", StringComparison.OrdinalIgnoreCase) ||
        text.Contains("7 дней", StringComparison.OrdinalIgnoreCase) ||
        text.Contains("12 месяцев", StringComparison.OrdinalIgnoreCase) ||
        text.Contains("last", StringComparison.OrdinalIgnoreCase) ||
        text.Contains("rolling", StringComparison.OrdinalIgnoreCase);

    private static bool IsCalendarPeriodAnswer(string text) =>
        text.Contains("календар", StringComparison.OrdinalIgnoreCase) ||
        text.Contains("прошл", StringComparison.OrdinalIgnoreCase) ||
        text.Contains("предыдущ", StringComparison.OrdinalIgnoreCase) ||
        text.Contains("previous", StringComparison.OrdinalIgnoreCase);

    private static readonly string[] CurrentYearPhrases =
    {
        "этот год", "текущий год", "this year", "current year"
    };

    private static readonly string[] PreviousYearPhrases =
    {
        "прошлый год", "предыдущий год", "last year", "previous year"
    };

    private static readonly string[] CurrentMonthPhrases =
    {
        "этот месяц", "текущий месяц", "this month", "current month"
    };

    private static readonly string[] PreviousMonthPhrases =
    {
        "прошлый месяц", "предыдущий месяц", "last month", "previous month"
    };

    private static readonly string[] CurrentWeekPhrases =
    {
        "эта неделя", "текущая неделя", "this week", "current week"
    };

    private static readonly string[] PreviousWeekPhrases =
    {
        "прошлая неделя", "предыдущая неделя", "last week", "previous week"
    };

    private static readonly string[] RollingThirtyDaysPhrases =
    {
        "последние 30 дней", "последних 30 дней", "30 дней", "last 30 days", "rolling 30 days"
    };

    private static readonly string[] RollingSevenDaysPhrases =
    {
        "последние 7 дней", "последних 7 дней", "7 дней", "last 7 days", "rolling 7 days"
    };

    private static readonly string[] RollingTwelveMonthsPhrases =
    {
        "последние 12 месяцев", "последних 12 месяцев", "12 месяцев", "last 12 months", "rolling 12 months"
    };
}
