using Avalonia.Controls;
using DriveeDataSpace.Core.Models;
using DriveeDataSpace.DriveeDataSpace.Desktop.Models;

namespace DriveeDataSpace.Desktop.Views;

public partial class ResultTableView : UserControl
{
    public ResultTableView()
    {
        InitializeComponent();
    }

    /// <summary>
    /// Динамически создаёт колонки и заполняет DataGrid.
    /// Вызывается из ChatMessageView после получения данных.
    /// </summary>
    public void SetData(QueryResult data)
    {
        var grid = this.FindControl<DataGrid>("Grid")!;
        grid.Columns.Clear();

        for (var i = 0; i < data.Columns.Count; i++)
        {
            var idx = i; // захватываем для closure
            grid.Columns.Add(new DataGridTextColumn
            {
                Header = data.Columns[idx],
                Binding = new Avalonia.Data.Binding($"[{idx}]") { Converter = CellConverter.Instance },
                FontSize = 13
            });
        }

        // DataGrid принимает IList<List<object?>>
        grid.ItemsSource = data.Rows.Take(200).ToList();
    }
}

/// <summary>Форматирует числа красиво: 1234567 → 1 234 567</summary>
internal class CellConverter : Avalonia.Data.Converters.IValueConverter
{
    public static readonly CellConverter Instance = new();

    public object? Convert(object? value, System.Type targetType, object? parameter, System.Globalization.CultureInfo culture)
    {
        return value switch
        {
            null    => "—",
            double d => d.ToString("0.##"),
            float f  => f.ToString("0.##"),
            decimal m => m.ToString("0.##"),
            long l   => l.ToString("N0"),
            int i    => i.ToString("N0"),
            _        => value.ToString() ?? ""
        };
    }

    public object? ConvertBack(object? value, System.Type targetType, object? parameter, System.Globalization.CultureInfo culture)
        => throw new System.NotImplementedException();
}