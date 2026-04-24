using System.Text.RegularExpressions;

namespace DriveeDataSpace.Web.Services;

public sealed record SqlGuardReport(
    bool Ok,
    string? Reason,
    List<string> AppliedChecks);

public class SqlGuard
{
    private static readonly string[] ForbiddenKeywords =
    {
        "drop", "delete", "update", "insert", "alter", "create", "truncate",
        "attach", "detach", "pragma", "replace", "grant", "revoke", "exec", "execute"
    };

    private static readonly HashSet<string> SqlKeywords = new(StringComparer.OrdinalIgnoreCase)
    {
        "select", "from", "where", "group", "by", "order", "limit", "as", "and", "or", "in", "not", "null",
        "union", "all", "case", "when", "then", "else", "end", "round", "count", "sum", "avg", "cast", "strftime",
        "date", "integer", "asc", "desc", "nullif", "on"
    };

    private readonly SemanticLayer _semanticLayer;
    private readonly int _maxRows;

    public SqlGuard(SemanticLayer semanticLayer, IConfiguration configuration)
    {
        _semanticLayer = semanticLayer;
        _maxRows = int.TryParse(configuration["Data:MaxRows"], out var maxRows) ? maxRows : 10000;
    }

    public SqlGuardReport Validate(string sql, IReadOnlyDictionary<string, object?> parameters, ValidatedIntent intent)
    {
        var checks = new List<string>();
        if (string.IsNullOrWhiteSpace(sql))
            return new SqlGuardReport(false, "Пустой SQL.", checks);

        var trimmedSql = sql.Trim();
        var inspectionSql = StripQuotedContent(trimmedSql);
        if (!Regex.IsMatch(trimmedSql, @"^\s*SELECT\b", RegexOptions.IgnoreCase))
            return new SqlGuardReport(false, "Разрешены только SELECT-запросы.", checks);
        checks.Add("only_select");

        if (Regex.IsMatch(trimmedSql, @"\bselect\s+\*", RegexOptions.IgnoreCase))
            return new SqlGuardReport(false, "Запрещён SELECT *.", checks);
        checks.Add("no_select_star");

        var semiColonCount = trimmedSql.TrimEnd(';').Count(character => character == ';');
        if (semiColonCount > 0)
            return new SqlGuardReport(false, "Множественные statements запрещены.", checks);
        checks.Add("single_statement");

        foreach (var keyword in ForbiddenKeywords)
        {
            if (Regex.IsMatch(inspectionSql, $@"\b{keyword}\b", RegexOptions.IgnoreCase))
                return new SqlGuardReport(false, $"Запрещённая команда: {keyword.ToUpperInvariant()}.", checks);
        }
        checks.Add("no_mutations");

        var limitMatch = Regex.Match(trimmedSql, @"\blimit\s+(\d+)\b", RegexOptions.IgnoreCase);
        if (!limitMatch.Success)
            return new SqlGuardReport(false, "SQL должен содержать LIMIT.", checks);
        if (!int.TryParse(limitMatch.Groups[1].Value, out var parsedLimit) || parsedLimit <= 0 || parsedLimit > _maxRows)
            return new SqlGuardReport(false, $"LIMIT должен быть в диапазоне 1..{_maxRows}.", checks);
        checks.Add("bounded_limit");

        var allowedTables = _semanticLayer.Sources.Select(source => source.Table).ToHashSet(StringComparer.OrdinalIgnoreCase);
        var referencedTables = Regex.Matches(trimmedSql, @"\b(?:from|join)\s+([a-z_][a-z0-9_]*)\b", RegexOptions.IgnoreCase)
            .Select(match => match.Groups[1].Value)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();
        if (referencedTables.Any(table => !allowedTables.Contains(table)))
            return new SqlGuardReport(false, $"Обнаружена неизвестная таблица: {string.Join(", ", referencedTables.Where(table => !allowedTables.Contains(table)))}.", checks);
        checks.Add("known_tables");

        var sanitizedSql = Regex.Replace(trimmedSql, @"'[^']*'", " ", RegexOptions.CultureInvariant);
        var parameterNames = parameters.Keys.Select(key => key.TrimStart('$')).ToHashSet(StringComparer.OrdinalIgnoreCase);
        var allowedFields = new HashSet<string>(intent.Source.AllowedColumns, StringComparer.OrdinalIgnoreCase)
        {
            intent.Metric.Key,
            "period",
            "__sort"
        };

        foreach (var dimension in intent.Dimensions)
        {
            allowedFields.Add(dimension.Definition.Key);
            allowedFields.Add(dimension.Alias);
        }

        var tokens = Regex.Matches(sanitizedSql.ToLowerInvariant(), @"\b[a-z_][a-z0-9_]*\b", RegexOptions.CultureInvariant)
            .Select(match => match.Value)
            .Distinct(StringComparer.OrdinalIgnoreCase);

        var unknownTokens = tokens
            .Where(token =>
                !SqlKeywords.Contains(token) &&
                !allowedTables.Contains(token) &&
                !allowedFields.Contains(token) &&
                !parameterNames.Contains(token))
            .ToList();

        if (unknownTokens.Count > 0)
            return new SqlGuardReport(false, $"Обнаружены неизвестные поля или идентификаторы: {string.Join(", ", unknownTokens)}.", checks);
        checks.Add("known_fields");

        if (intent.Dimensions.Count > 2)
            return new SqlGuardReport(false, "Слишком тяжёлая группировка: поддерживается максимум две размерности.", checks);
        checks.Add("bounded_complexity");

        return new SqlGuardReport(true, null, checks);
    }

    private static string StripQuotedContent(string sql)
    {
        var withoutBlockComments = Regex.Replace(sql, @"/\*.*?\*/", " ", RegexOptions.Singleline | RegexOptions.CultureInvariant);
        var withoutLineComments = Regex.Replace(withoutBlockComments, @"--.*?$", " ", RegexOptions.Multiline | RegexOptions.CultureInvariant);
        var withoutStrings = Regex.Replace(withoutLineComments, @"'([^']|'')*'", " ", RegexOptions.CultureInvariant);
        var withoutQuotedIdentifiers = Regex.Replace(withoutStrings, @"""([^""]|"""")*""", " ", RegexOptions.CultureInvariant);
        return withoutQuotedIdentifiers;
    }

    public static void EnsureDifferentIntentProducesDifferentSql(
        ValidatedIntent previousIntent,
        BuiltSql previousSql,
        ValidatedIntent currentIntent,
        BuiltSql currentSql)
    {
        if (string.Equals(previousIntent.DiscriminatorKey, currentIntent.DiscriminatorKey, StringComparison.Ordinal))
            return;

        if (string.Equals(previousSql.Signature, currentSql.Signature, StringComparison.Ordinal))
        {
            throw new InvalidOperationException(
                "Guardrails: разные intent не должны приводить к одинаковому SQL. Проверьте период, метрику, группировку, фильтры, сортировку, лимит или источник данных.");
        }
    }
}
