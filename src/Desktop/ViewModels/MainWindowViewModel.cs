using System.Collections.ObjectModel;
using CommunityToolkit.Mvvm.ComponentModel;
using CommunityToolkit.Mvvm.Input;
using DriveeDataSpace.Desktop.Models;
using DriveeDataSpace.Desktop.Services;
using System.Collections.Generic;
using System.Threading.Tasks;
using System;
using System.Linq;
using ScottPlot;
namespace DriveeDataSpace.Desktop.ViewModels;

public partial class MainWindowViewModel : ObservableObject
{
    private readonly DesktopApiClient _api;
    public MainWindowViewModel(DesktopApiClient api) => _api = api;

    [ObservableProperty] private string _queryText = string.Empty;
    [ObservableProperty] private string _status = "Готов к запросу";
    [ObservableProperty] private bool _isLoading;
    [ObservableProperty] private QueryResponse? _lastResult;
    [ObservableProperty] private ObservableCollection<QueryResponse> _savedReports = new();
    [ObservableProperty] private List<double> _chartXValues = new();
    [ObservableProperty] private List<double> _chartYValues = new();
    [ObservableProperty] private string _chartTitle = string.Empty;
    [ObservableProperty] private int _selectedTabIndex = 0;
    [ObservableProperty] private string _chartType = "bar";
    public Action? OnResultChanged { get; set; }
    public List<string> ChartTypes => new() { "bar", "line", "pie" };

    partial void OnChartTypeChanged(string value) => UpdateChart(value);

    [RelayCommand(CanExecute = nameof(CanExecuteQuery))]
    private async Task ExecuteQueryAsync()
    {
        if (string.IsNullOrWhiteSpace(QueryText)) return;
        IsLoading = true;
        LastResult = null;

        try
        {
            Status = "📥 Запрос принят"; await Task.Delay(300);
            Status = "🧠 Интерпретация запроса..."; await Task.Delay(300);
            Status = "🔧 Построение SQL..."; await Task.Delay(300);
            Status = "🗄 Выполнение запроса..."; await Task.Delay(300);
            Status = "📊 Построение визуализации...";

            LastResult = await _api.ExecuteQueryAsync(QueryText);
            ChartType = LastResult.VisualizationHint ?? "bar";
            Status = LastResult.Intent.Confidence < 0.7 
                ? $"✅ Готово (Уверенность: {(LastResult.Intent.Confidence*100):F0}%) - рекомендуется уточнить" 
                : "✅ Готово";
        }
        catch (Exception ex)
        {
            Status = $"❌ Ошибка: {ex.Message}";
        }
        finally { IsLoading = false; }
    }

    private bool CanExecuteQuery => !IsLoading && !string.IsNullOrWhiteSpace(QueryText);

    [RelayCommand(CanExecute = nameof(CanSaveReport))]
    private async Task SaveReportAsync()
    {
        if (LastResult == null) return;
        LastResult = await _api.SaveReportAsync(LastResult);
        await RefreshReportsAsync();
    }
    private bool CanSaveReport => !IsLoading && LastResult != null;

    [RelayCommand]
    private async Task RerunReportAsync(QueryResponse report)
    {
        IsLoading = true;
        QueryText = report.Intent.Intent; // Для демо подставляем, в реальности храним исходный текст
        LastResult = await _api.RerunReportAsync(report.Id);
        ChartType = LastResult.VisualizationHint ?? "bar";
        Status = "🔄 Отчёт переисполнен";
        IsLoading = false;
    }

    [RelayCommand]
    private async Task RefreshReportsAsync() => 
        SavedReports = new ObservableCollection<QueryResponse>(await _api.GetReportsAsync());
    private void PrepareChartData(QueryResponse result)
    {
        ChartXValues = new List<double>();
        ChartYValues = new List<double>();
        ChartTitle = $"{result.Intent.Metric} ({result.Intent.Aggregation})";

        // Пример: берём первые две колонки как X и Y
        // В реальности тут должен быть маппинг по именам колонок
        if (result.ResultTable.Count > 0 && result.ResultTable[0].Count >= 2)
        {
            var keys = result.ResultTable[0].Keys.ToList();
            foreach (var row in result.ResultTable)
            {
                // Простая конвертация: если число — берём, иначе пропускаем
                if (row.TryGetValue(keys[1], out var yVal) && 
                    double.TryParse(yVal?.ToString(), out var y))
                {
                    var xLabel = row[keys[0]]?.ToString() ?? "";
                    ChartXValues.Add(ChartXValues.Count); // индекс как X
                    ChartYValues.Add(y);
                }
            }
        }
    }
    partial void OnLastResultChanged(QueryResponse? value)
    {
        if (value?.ResultTable == null) return;
        
        // Преобразуем данные таблицы в точки для графика
        PrepareChartData(value);
        
        // Уведомляем View, что данные готовы для отрисовки
        OnResultChanged?.Invoke();
    }
    
    private void UpdateChart(string type)
    {
        
    }

    public void LoadInitialData() => _ = RefreshReportsAsync();
}