using Avalonia.Controls;
using Avalonia.Interactivity;
using DriveeDataSpace.Desktop.ViewModels;
using ScottPlot;
namespace DriveeDataSpace.Desktop.Views;

public partial class MainWindow : Window
{
    public MainWindow()
    {
        InitializeComponent();
        if (DataContext is MainWindowViewModel vm)
        {
            vm.OnResultChanged = UpdatePlot;
        }
    }
    private void UpdatePlot()
    {
        if (DataContext is not MainWindowViewModel vm || vm.LastResult == null) 
            return;

        // Очищаем и настраиваем график
        PlotControl.Plot.Clear();
        
        // Рисуем столбчатую диаграмму (Bar)
        var bars = PlotControl.Plot.Add.Bars(vm.ChartYValues.ToArray());
        bars.Color = Colors.Blue.WithAlpha(0.7);
        
        // Подписи осей и заголовок
        PlotControl.Plot.Title(vm.ChartTitle);
        PlotControl.Plot.Axes.Bottom.Label.Text = "№";
        PlotControl.Plot.Axes.Left.Label.Text = "Значение";
        
        // Авто-масштаб и обновление
        PlotControl.Plot.Axes.AutoScale();
        PlotControl.Refresh();
    }
    private void TextBox_KeyDown(object? sender, Avalonia.Input.KeyEventArgs e)
    {
        if (e.Key == Avalonia.Input.Key.Enter && !e.Handled)
        {
            var vm = DataContext as DriveeDataSpace.Desktop.ViewModels.MainWindowViewModel;
            if (vm?.ExecuteQueryCommand.CanExecute(null) == true)
            {
                vm.ExecuteQueryCommand.Execute(null);
                e.Handled = true;
            }
        }
    }
}