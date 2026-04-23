using System.Globalization;
using Avalonia.Data.Converters;
using Avalonia.Media;
using DriveeDataSpace.DriveeDataSpace.Desktop.Models;

namespace DriveeDataSpace.Desktop.Views;

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
            ? Brush.Parse("#ecfdf5")
            : c >= 0.4
                ? Brush.Parse("#fffbeb")
                : Brush.Parse("#fef2f2"));

    public static readonly IValueConverter ConfidenceFgConverter =
        new FuncConverter<double, IBrush>(c => c >= 0.7
            ? Brush.Parse("#047857")
            : c >= 0.4
                ? Brush.Parse("#92400e")
                : Brush.Parse("#b91c1c"));

    public static readonly IValueConverter ConfidenceBorderConverter =
        new FuncConverter<double, IBrush>(c => c >= 0.7
            ? Brush.Parse("#a7f3d0")
            : c >= 0.4
                ? Brush.Parse("#fde68a")
                : Brush.Parse("#fecaca"));
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