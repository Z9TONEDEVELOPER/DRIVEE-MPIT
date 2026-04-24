using System.Collections.ObjectModel;
using System.Text.Json;
using CommunityToolkit.Mvvm.ComponentModel;
using CommunityToolkit.Mvvm.Input;
using DriveeDataSpace.Core.Models;
using DriveeDataSpace.DriveeDataSpace.Desktop.Models;
using DriveeDataSpace.DriveeDataSpace.Desktop.Services;

namespace DriveeDataSpace.DriveeDataSpace.Desktop.ViewModels;

public partial class ChatViewModel : ViewModelBase
{
    private readonly BiApiClient _api;

    public ChatViewModel(BiApiClient api)
    {
        _api = api;
    }

    // ── State ────────────────────────────────────────────────────────────────
    [ObservableProperty] private string _inputText = "";
    [ObservableProperty] private bool _isBusy;
    [ObservableProperty] private string _thinkingLabel = "Думаю…";
    [ObservableProperty] private ObservableCollection<ChatMessage> _messages = new();
    [ObservableProperty] private ObservableCollection<Report> _reports = new();
    [ObservableProperty] private string _selectedVisualization = "bar";

    public bool IsEmpty => Messages.Count == 0;

    partial void OnMessagesChanged(ObservableCollection<ChatMessage> value)
        => OnPropertyChanged(nameof(IsEmpty));

    // ── Templates ────────────────────────────────────────────────────────────
    public IReadOnlyList<string> Templates { get; } = new[]
    {
        "Количество заказов по дням за последние 30 дней",
        "Выручка по месяцам",
        "Распределение заказов по часам",
        "Доля отмен по дням за последний месяц",
        "Средний чек по статусам",
        "Сравни выручку за этот и прошлый месяц",
        "Среднее расстояние и длительность поездок"
    };

    // ── Commands ─────────────────────────────────────────────────────────────
    [RelayCommand]
    public async Task SendAsync()
    {
        var q = InputText?.Trim() ?? "";
        if (string.IsNullOrWhiteSpace(q) || IsBusy) return;
        InputText = "";
        await RunQueryAsync(q);
    }

    [RelayCommand]
    public void UseTemplate(string template)
    {
        if (IsBusy) return;
        InputText = template;
    }

    [RelayCommand]
    public async Task RunTemplateAsync(string template)
    {
        if (IsBusy) return;
        await RunQueryAsync(template);
    }

    [RelayCommand]
    public void ClearChat()
    {
        Messages.Clear();
        OnPropertyChanged(nameof(IsEmpty));
    }

    [RelayCommand]
    public async Task OpenReportAsync(Report report)
    {
        if (IsBusy) return;
        Messages.Add(new ChatMessage { Role = ChatRole.User, Text = $"[↻ отчёт] {report.UserQuery}" });
        IsBusy = true;
        ThinkingLabel = "Переисполняю отчёт…";
        OnPropertyChanged(nameof(IsEmpty));

        try
        {
            var pr = await _api.RerunReportAsync(report.Id);
            pr.UserQuery = report.UserQuery;
            AddResultMessage(pr, report.Name, $"#{report.Id}");
        }
        catch (Exception ex)
        {
            Messages.Add(new ChatMessage { Role = ChatRole.Bot, Text = "Не удалось открыть отчёт: " + ex.Message });
            OnPropertyChanged(nameof(IsEmpty));
        }
        finally
        {
            IsBusy = false;
            ThinkingLabel = "Думаю…";
        }
    }

    [RelayCommand]
    public async Task DeleteReportAsync(Report report)
    {
        try
        {
            await _api.DeleteReportAsync(report.Id);
            Reports.Remove(report);
        }
        catch { /* ignore */ }
    }

    [RelayCommand]
    public async Task SaveReportAsync(ChatMessage msg)
    {
        if (msg.Result?.Intent == null || msg.Result.Sql == null) return;
        try
        {
            var req = new SaveReportRequest(
                string.IsNullOrWhiteSpace(msg.ReportName) ? SuggestName(msg.Result) : msg.ReportName.Trim(),
                msg.Result.UserQuery,
                System.Text.Json.JsonSerializer.Serialize(msg.Result.Intent),
                msg.Result.Sql,
                msg.Visualization);
            var saved = await _api.SaveReportAsync(req);
            msg.SavedAs = $"#{saved.Id}";
            Reports.Insert(0, saved);
            // Уведомляем UI об изменении конкретного сообщения
            var idx = Messages.IndexOf(msg);
            if (idx >= 0) { Messages.RemoveAt(idx); Messages.Insert(idx, msg); }
        }
        catch (Exception ex)
        {
            msg.SavedAs = null;
            _ = ex;
        }
    }

    // ── Internals ────────────────────────────────────────────────────────────
    private async Task RunQueryAsync(string q)
    {
        Messages.Add(new ChatMessage { Role = ChatRole.User, Text = q });
        OnPropertyChanged(nameof(IsEmpty));
        IsBusy = true;
        ThinkingLabel = "Интерпретирую запрос…";

        try
        {
            var history = BuildHistory();
            var previousIntent = Messages
                .Where(message => message.Role == ChatRole.Result && message.Result?.Intent?.Kind == QueryIntentKinds.Query)
                .Select(message => message.Result!.Intent)
                .LastOrDefault();

            var pr = await _api.RunQueryAsync(q, history, previousIntent);
            pr.UserQuery = q;

            if (pr.IsChat)
                Messages.Add(new ChatMessage { Role = ChatRole.Bot, Text = pr.ChatReply });
            else
                AddResultMessage(pr, SuggestName(pr));
            OnPropertyChanged(nameof(IsEmpty));
        }
        catch (Exception ex)
        {
            Messages.Add(new ChatMessage { Role = ChatRole.Bot, Text = $"Ошибка: {ex.Message}" });
            OnPropertyChanged(nameof(IsEmpty));
        }
        finally
        {
            IsBusy = false;
            ThinkingLabel = "Думаю…";
        }
    }

    private void AddResultMessage(PipelineResult pr, string name, string? savedAs = null)
    {
        var msg = new ChatMessage
        {
            Role = ChatRole.Result,
            Result = pr,
            Visualization = pr.Visualization,
            ReportName = name,
            SavedAs = savedAs
        };
        Messages.Add(msg);
        OnPropertyChanged(nameof(IsEmpty));
    }

    public async Task LoadReportsAsync()
    {
        try
        {
            var list = await _api.GetReportsAsync();
            Reports = new ObservableCollection<Report>(list);
        }
        catch { /* ignore on load */ }
    }

    private List<ChatTurn> BuildHistory()
    {
        var turns = new List<ChatTurn>();
        foreach (var message in Messages.TakeLast(8))
        {
            if (message.Role == ChatRole.User && !string.IsNullOrWhiteSpace(message.Text))
                turns.Add(new ChatTurn("user", message.Text));
            else if (message.Role == ChatRole.Bot && !string.IsNullOrWhiteSpace(message.Text))
                turns.Add(new ChatTurn("assistant", message.Text));
            else if (message.Role == ChatRole.Result && message.Result?.Intent != null)
                turns.Add(new ChatTurn("assistant", JsonSerializer.Serialize(message.Result.Intent)));
        }

        if (turns.Count > 0 && turns[^1].Role == "user")
            turns.RemoveAt(turns.Count - 1);

        return turns;
    }

    private static string SuggestName(PipelineResult pr)
    {
        if (pr.Intent == null) return pr.UserQuery;
        var parts = new System.Collections.Generic.List<string> { pr.Intent.Metric ?? "report" };
        if (!string.IsNullOrEmpty(pr.Intent.GroupBy)) parts.Add($"× {pr.Intent.GroupBy}");
        if (!string.IsNullOrEmpty(pr.Intent.Period)) parts.Add(pr.Intent.Period!);
        return string.Join(" ", parts);
    }
}
