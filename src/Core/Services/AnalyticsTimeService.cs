using System.Text.RegularExpressions;

namespace NexusDataSpace.Core.Services;

public sealed record AnalyticsTimeContext(
    DateTime SystemTodayUtc,
    DateTime? LatestDataTimestampUtc,
    DateTime AnchorDateUtc,
    bool UsesDataAnchor);

public class AnalyticsTimeService
{
    private readonly DataSourceService _dataSources;

    public AnalyticsTimeService(DataSourceService dataSources)
    {
        _dataSources = dataSources;
    }

    public AnalyticsTimeContext GetContext(string? table = null, string? dateColumn = null)
    {
        var systemToday = DateTime.UtcNow.Date;
        var latestDataTimestamp = TryGetLatestTimestampUtc(table, dateColumn);
        var anchorDate = latestDataTimestamp.HasValue && latestDataTimestamp.Value.Date < systemToday
            ? latestDataTimestamp.Value.Date
            : systemToday;

        return new AnalyticsTimeContext(
            systemToday,
            latestDataTimestamp,
            anchorDate,
            latestDataTimestamp.HasValue && latestDataTimestamp.Value.Date < systemToday);
    }

    private DateTime? TryGetLatestTimestampUtc(string? table, string? dateColumn)
    {
        if (!IsSafeIdentifier(table) || !IsSafeIdentifier(dateColumn))
            return null;

        try
        {
            var dataSource = _dataSources.GetActive();
            using var connection = _dataSources.OpenReadOnlyConnection(dataSource);
            using var command = connection.CreateCommand();
            command.CommandText = $"SELECT MAX({dateColumn}) FROM {table}";
            var value = command.ExecuteScalar()?.ToString();

            return DateTime.TryParse(value, out var parsed)
                ? DateTime.SpecifyKind(parsed, DateTimeKind.Utc)
                : null;
        }
        catch
        {
            return null;
        }
    }

    private static bool IsSafeIdentifier(string? value) =>
        !string.IsNullOrWhiteSpace(value) &&
        Regex.IsMatch(
            value,
            @"^\s*(?:""(?:""""|[^""])+""|[A-Za-z_][A-Za-z0-9_]*)(?:\s*\.\s*(?:""(?:""""|[^""])+""|[A-Za-z_][A-Za-z0-9_]*))*\s*$",
            RegexOptions.CultureInvariant);
}
