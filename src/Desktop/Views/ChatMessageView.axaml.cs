using Avalonia.Controls;
using Avalonia.Interactivity;
using NexusDataSpace.Core.Models;
using NexusDataSpace.Desktop.Models;
using NexusDataSpace.Desktop.ViewModels;

namespace NexusDataSpace.Desktop.Views;

public partial class ChatMessageView : UserControl
{
    private string? _renderedKey;

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
            RenderResultOnce(msg);
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
        {
            if (viz == "table")
            {
                ChartView.IsVisible = false;
                TableView.IsVisible = true;
                return;
            }

            var rendered = ChartView.Render(msg.Result.Result, viz);
            ChartView.IsVisible = rendered;
            TableView.IsVisible = !rendered;
            if (!rendered)
                msg.Visualization = "table";
        }
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

    private void RenderResultOnce(ChatMessage msg)
    {
        if (msg.Result?.Result == null)
            return;

        var key = $"{msg.Id}:{msg.Visualization}:{msg.Result.Result.RowCount}:{msg.Result.Result.Columns.Count}";
        if (string.Equals(_renderedKey, key, StringComparison.Ordinal))
            return;

        TableView.SetData(msg.Result.Result);

        if (msg.Visualization == "table")
        {
            ChartView.IsVisible = false;
            TableView.IsVisible = true;
            _renderedKey = key;
            return;
        }

        var rendered = ChartView.Render(msg.Result.Result, msg.Visualization);
        ChartView.IsVisible = rendered;
        TableView.IsVisible = !rendered;
        if (!rendered)
        {
            msg.Visualization = "table";
            UpdateVizTabStyles("table");
        }

        _renderedKey = key;
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
