using Avalonia.Controls;
using NexusDataSpace.Core.Models;
using NexusDataSpace.Desktop.Models;
using ScottPlot.Avalonia;

namespace NexusDataSpace.Desktop.Views;

public partial class ResultChartView : UserControl
{
    private AvaPlot? _plot;
    private string? _lastRenderKey;

    public ResultChartView()
    {
        InitializeComponent();
        _plot = this.FindControl<AvaPlot>("Plot");
    }

    public bool Render(QueryResult data, string chartType)
    {
        if (_plot == null || data.Rows.Count == 0 || data.Columns.Count == 0)
            return false;

        var renderKey = BuildRenderKey(data, chartType);
        if (string.Equals(_lastRenderKey, renderKey, StringComparison.Ordinal))
            return true;

        try
        {
            _plot.Plot.Clear();

            // Извлекаем labels (col 0) и values (col 1)
            string[] labels;
            double[] values;

            if (data.Columns.Count >= 2)
            {
                labels = data.Rows.Select(r => FormatLabel(r[0])).ToArray();
                values = data.Rows.Select(r => ToDouble(r[1])).ToArray();
            }
            else
            {
                labels = new[] { data.Columns[0] };
                values = new[] { ToDouble(data.Rows[0][0]) };
            }

            // Цветовая палитра совпадает с Web
            var palette = new[]
            {
                new ScottPlot.Color(59, 130, 246),
                new ScottPlot.Color(34, 211, 238),
                new ScottPlot.Color(99, 102, 241),
                new ScottPlot.Color(0, 209, 255),
                new ScottPlot.Color(56, 189, 248),
                new ScottPlot.Color(34, 197, 94),
                new ScottPlot.Color(245, 158, 11),
                new ScottPlot.Color(239, 68, 68),
            };

            switch (chartType)
            {
                case "bar":
                    RenderBar(labels, values, palette[0]);
                    break;
                case "line":
                    RenderLine(labels, values, palette[0]);
                    break;
                case "pie":
                    RenderPie(labels, values, palette);
                    break;
                default:
                    RenderBar(labels, values, palette[0]);
                    break;
            }

            _plot.Plot.FigureBackground.Color = new ScottPlot.Color(17, 26, 46);
            _plot.Plot.DataBackground.Color = new ScottPlot.Color(15, 23, 42);
            _plot.Plot.Axes.Color(new ScottPlot.Color(148, 163, 184));
            _plot.Plot.Grid.MajorLineColor = new ScottPlot.Color(255, 255, 255, 13);
            _plot.Refresh();
            _lastRenderKey = renderKey;
            return true;
        }
        catch
        {
            _plot.Plot.Clear();
            _lastRenderKey = null;
            return false;
        }
    }

    private void RenderBar(string[] labels, double[] values, ScottPlot.Color color)
    {
        var bars = _plot!.Plot.Add.Bars(values);
        bars.Color = color;

        // Подписи по X
        _plot.Plot.Axes.Bottom.TickLabelStyle.Rotation = labels.Length > 6 ? -45 : 0;
        _plot.Plot.Axes.Bottom.TickLabelStyle.FontSize = 11;
        _plot.Plot.Axes.Left.TickLabelStyle.FontSize = 11;
        _plot.Plot.ShowGrid();
        _plot.Plot.Axes.AutoScale();
    }

    private void RenderLine(string[] labels, double[] values, ScottPlot.Color color)
    {
        var xs = Enumerable.Range(0, values.Length).Select(i => (double)i).ToArray();
        var scatter = _plot!.Plot.Add.Scatter(xs, values);
        scatter.Color = color;
        scatter.LineWidth = 2;
        scatter.MarkerSize = 5;

        _plot.Plot.Axes.Bottom.TickLabelStyle.Rotation = labels.Length > 6 ? -45 : 0;
        _plot.Plot.Axes.Bottom.TickLabelStyle.FontSize = 11;
        _plot.Plot.ShowGrid();
        _plot.Plot.Axes.AutoScale();
    }

    private void RenderPie(string[] labels, double[] values, ScottPlot.Color[] palette)
    {
        var slices = labels.Select((l, i) => new ScottPlot.PieSlice
        {
            Value = values[i],
            Label = l,
            FillColor = palette[i % palette.Length]
        }).ToList();

        var pie = _plot!.Plot.Add.Pie(slices);
        pie.ExplodeFraction = 0.02;
        _plot.Plot.HideGrid();
        _plot.Plot.Axes.AutoScale();
    }

    // ── Helpers ──────────────────────────────────────────────────────────────
    private static double ToDouble(object? v) => v switch
    {
        null    => 0,
        double d => d,
        float f  => f,
        decimal m => (double)m,
        long l   => l,
        int i    => i,
        _        => double.TryParse(v.ToString(), out var r) ? r : 0
    };

    private static string FormatLabel(object? v)
    {
        if (v == null) return "";
        var s = v.ToString() ?? "";
        // Дата типа 2024-03-15 → 15.03.24
        if (s.Length >= 10 && s[4] == '-' && s[7] == '-')
            return $"{s[8..10]}.{s[5..7]}.{s[2..4]}";
        return s;
    }

    private static string BuildRenderKey(QueryResult data, string chartType)
    {
        var first = data.Rows.Count > 0 && data.Rows[0].Count > 0 ? data.Rows[0][0]?.ToString() : "";
        var lastRow = data.Rows.Count > 0 ? data.Rows[^1] : null;
        var last = lastRow is { Count: > 0 } ? lastRow[^1]?.ToString() : "";
        return $"{chartType}|{data.Columns.Count}|{data.Rows.Count}|{first}|{last}";
    }
}
