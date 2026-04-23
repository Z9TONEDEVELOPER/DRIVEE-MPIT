using System.Text.RegularExpressions;

namespace DriveeDataSpace.Web.Services;

public static class SqlGuard
{
    private static readonly string[] Forbidden =
    {
        "drop", "delete", "update", "insert", "alter", "create", "truncate",
        "attach", "detach", "pragma", "replace", "grant", "revoke", "exec", "execute"
    };

    public static (bool ok, string? reason) Validate(string sql)
    {
        if (string.IsNullOrWhiteSpace(sql))
            return (false, "Пустой SQL");

        var trimmed = sql.TrimStart();
        if (!Regex.IsMatch(trimmed, @"^\s*SELECT\b", RegexOptions.IgnoreCase))
            return (false, "Разрешены только SELECT-запросы");

        var semiCount = sql.TrimEnd().TrimEnd(';').Count(c => c == ';');
        if (semiCount > 0)
            return (false, "Множественные statements запрещены");

        var lower = sql.ToLowerInvariant();
        foreach (var kw in Forbidden)
        {
            if (Regex.IsMatch(lower, $@"\b{kw}\b"))
                return (false, $"Запрещённая команда: {kw.ToUpper()}");
        }

        if (!Regex.IsMatch(lower, @"\blimit\b"))
            return (false, "Запрос должен содержать LIMIT");

        return (true, null);
    }
}
