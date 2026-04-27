using System.Globalization;
using Avalonia.Data.Converters;
using Avalonia.Media;
using NexusDataSpace.Core.Models;
using NexusDataSpace.Desktop.Models;

namespace NexusDataSpace.Desktop.Views;

/// <summary>
/// Все конвертеры доступны как static свойства класса Converters
/// и используются в AXAML через {x:Static views:Converters.XxxConverter}
/// </summary>
public static class Converters
{
    // ── Role converters ───────────────────────────────────────────────────
    public static readonly IValueConverter IsUserRoleConverter =
        new FuncConverter<ChatRole, bool>(r => r == ChatRole.User);

    public static readonly IValueConverter IsBotRoleConverter =
        new FuncConverter<ChatRole, bool>(r => r == ChatRole.Bot);

    public static readonly IValueConverter IsResultRoleConverter =
        new FuncConverter<ChatRole, bool>(r => r == ChatRole.Result);

    // ── Null/empty converters ─────────────────────────────────────────────
    public static readonly IValueConverter IsNullConverter =
        new FuncConverter<object?, bool>(v => v == null);

    public static readonly IValueConverter IsNotNullConverter =
        new FuncConverter<object?, bool>(v => v != null);

    public static readonly IValueConverter IsZeroConverter =
        new FuncConverter<int, bool>(v => v == 0);

    public static readonly IValueConverter IsNotZeroConverter =
        new FuncConverter<int, bool>(v => v != 0);

    public static readonly IValueConverter LocalDateTimeConverter =
        new FuncConverter<DateTime, string>(v => v.ToLocalTime().ToString("g", CultureInfo.CurrentCulture));

    // ── QueryResult ───────────────────────────────────────────────────────
    public static readonly IValueConverter HasRowsConverter =
        new FuncConverter<QueryResult?, bool>(r => r != null && r.RowCount > 0);

    public static readonly IValueConverter RowStatsConverter =
        new FuncConverter<QueryResult?, string>(r =>
            r == null ? "" : $"{r.RowCount} строк · {r.DurationMs} мс");

    // ── Confidence ────────────────────────────────────────────────────────
    public static readonly IValueConverter ConfidenceLabelConverter =
        new FuncConverter<double, string>(c => $"confidence {(int)(c * 100)}%");

    public static readonly IValueConverter ConfidenceBgConverter =
        new FuncConverter<double, IBrush>(c => c >= 0.7
            ? Brush.Parse("#1F22C55E")
            : c >= 0.4
                ? Brush.Parse("#1FF59E0B")
                : Brush.Parse("#1FEF4444"));

    public static readonly IValueConverter ConfidenceFgConverter =
        new FuncConverter<double, IBrush>(c => c >= 0.7
            ? Brush.Parse("#86EFAC")
            : c >= 0.4
                ? Brush.Parse("#FCD34D")
                : Brush.Parse("#FCA5A5"));

    public static readonly IValueConverter ConfidenceBorderConverter =
        new FuncConverter<double, IBrush>(c => c >= 0.7
            ? Brush.Parse("#5522C55E")
            : c >= 0.4
                ? Brush.Parse("#55F59E0B")
                : Brush.Parse("#55EF4444"));
}

// ── Generic functional converter ─────────────────────────────────────────────
internal sealed class FuncConverter<TIn, TOut>(Func<TIn, TOut> convert) : IValueConverter
{
    public object? Convert(object? value, Type targetType, object? parameter, CultureInfo culture)
    {
        try { return convert(value is TIn t ? t : default!); }
        catch { return default(TOut); }
    }

    public object? ConvertBack(object? value, Type targetType, object? parameter, CultureInfo culture)
        => throw new NotImplementedException();
}
