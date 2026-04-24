using Avalonia.Controls;
using Avalonia.Interactivity;
using DriveeDataSpace.DriveeDataSpace.Desktop.Models;
using DriveeDataSpace.DriveeDataSpace.Desktop.ViewModels;

namespace DriveeDataSpace.Desktop.Views;

public partial class ChatMessageView : UserControl
{
    public ChatMessageView()
    {
        InitializeComponent();
        DataContextChanged += (_, _) => OnDataContextChanged();
    }

    private void OnDataContextChanged()
    {
        if (DataContext is ChatMessage msg)
        {
            // Установить активный viz-таб
            UpdateVizTabStyles(msg.Visualization);
            // Отрисовать начальный чарт
            if (msg.Result?.Result != null)
            {
                TableView.SetData(msg.Result.Result);
                ChartView.IsVisible = msg.Visualization != "table";
                TableView.IsVisible = msg.Visualization == "table";
                if (msg.Visualization != "table")
                    ChartView.Render(msg.Result.Result, msg.Visualization);
            }
        }
    }

    // ── Insight tabs (reasoning / sql) ──────────────────────────────────────
    private void InsightTab_Click(object? sender, RoutedEventArgs e)
    {
        if (sender is not Button btn || DataContext is not ChatMessage msg) return;
        var tab = btn.Tag?.ToString();

        var same = msg.InsightTab == tab;
        msg.InsightTab = same ? null : tab;

        ReasoningPanel.IsVisible = msg.InsightTab == "reasoning";
        SqlPanel.IsVisible = msg.InsightTab == "sql";

        // Обновляем стили кнопок
        foreach (var child in (btn.Parent as StackPanel)?.Children ?? [])
        {
            if (child is Button b)
            {
                var isActive = !same && b.Tag?.ToString() == tab;
                if (isActive) b.Classes.Add("active");
                else b.Classes.Remove("active");
            }
        }
    }

    // ── Tech details toggle ──────────────────────────────────────────────────
    private void TechDetails_Click(object? sender, RoutedEventArgs e)
    {
        TechPanel.IsVisible = !TechPanel.IsVisible;
        if (sender is Button b)
            b.Content = TechPanel.IsVisible ? "Технические детали ▲" : "Технические детали ▼";
    }

    // ── Visualization tab switch ─────────────────────────────────────────────
    private void VizTab_Click(object? sender, RoutedEventArgs e)
    {
        if (sender is not Button btn || DataContext is not ChatMessage msg) return;
        var viz = btn.Tag?.ToString() ?? "bar";
        msg.Visualization = viz;

        UpdateVizTabStyles(viz);

        if (msg.Result?.Result != null)
            ChartView.Render(msg.Result.Result, viz);

        ChartView.IsVisible = viz != "table";
        TableView.IsVisible = viz == "table";
    }

    private void UpdateVizTabStyles(string activeViz)
    {
        // Найти родительский StackPanel с viz-tab кнопками
        if (this.FindControl<StackPanel>("VizTabsPanel") is { } panel)
        {
            foreach (var child in panel.Children)
            {
                if (child is Button b)
                {
                    if (b.Tag?.ToString() == activeViz) b.Classes.Add("active");
                    else b.Classes.Remove("active");
                }
            }
        }
    }

    // ── Save report ──────────────────────────────────────────────────────────
    private async void SaveReport_Click(object? sender, RoutedEventArgs e)
    {
        if (DataContext is not ChatMessage msg) return;

        // Поднимаемся к MainWindow → MainWindowViewModel → Chat
        var win = TopLevel.GetTopLevel(this) as Avalonia.Controls.Window;
        if (win?.DataContext is MainWindowViewModel vm)
        {
            await vm.Chat.SaveReportCommand.ExecuteAsync(msg);
        }
    }
}
