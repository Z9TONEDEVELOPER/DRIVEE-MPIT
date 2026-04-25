using CommunityToolkit.Mvvm.ComponentModel;
using System.Collections.ObjectModel;
using CommunityToolkit.Mvvm.Input;
using DriveeDataSpace.Core.Models;
using DriveeDataSpace.DriveeDataSpace.Desktop.Services;

namespace DriveeDataSpace.DriveeDataSpace.Desktop.ViewModels;

public partial class MainWindowViewModel : ObservableObject
{
    private readonly BiApiClient _api;

    public ChatViewModel Chat { get; }

    [ObservableProperty] private bool _isSidebarCollapsed;
    [ObservableProperty] private string _currentPage = "workspace"; // "workspace" | "reports"
    [ObservableProperty] private string _displayName = "Desktop session";
    [ObservableProperty] private string _username = "desktop";
    [ObservableProperty] private bool _isAdmin;
    [ObservableProperty] private bool _isAuthenticated;
    [ObservableProperty] private string _loginUsername = "admin";
    [ObservableProperty] private string _loginPassword = "ChangeMe123!";
    [ObservableProperty] private string? _loginError;
    [ObservableProperty] private bool _isLoggingIn;
    [ObservableProperty] private ObservableCollection<AppUserSummary> _users = new();
    [ObservableProperty] private ObservableCollection<RegistrationRequest> _pendingRegistrations = new();
    [ObservableProperty] private string? _adminError;
    [ObservableProperty] private string? _adminNotice;
    [ObservableProperty] private LlmSettings? _llmSettings;
    [ObservableProperty] private string _selectedLlmProvider = LlmProviders.Local;
    [ObservableProperty] private bool _isSavingLlmSettings;

    public bool IsWorkspacePage => CurrentPage == "workspace";
    public bool IsReportsPage => CurrentPage == "reports";
    public bool IsUsersPage => CurrentPage == "users";
    public IReadOnlyList<string> LlmProviderOptions { get; } = LlmProviders.All;
    public string CurrentLlmProviderLabel => LlmSettings?.Provider ?? LlmProviders.Local;
    public string LocalLlmLabel => LlmSettings == null
        ? "local-model"
        : $"{LlmSettings.LocalModel} · {LlmSettings.LocalEndpoint}";
    public string GigaChatLabel => LlmSettings?.GigaChatModel ?? LlmProviders.GigaChat;
    public string GigaChatSecretStatus => LlmSettings?.IsGigaChatConfigured == true
        ? "ключ настроен"
        : "ключ не настроен";

    public MainWindowViewModel(BiApiClient api)
    {
        _api = api;
        Chat = new ChatViewModel(api);
    }

    [RelayCommand]
    private async Task NavigateToAsync(string page)
    {
        CurrentPage = page;
        if (page == "users")
            await LoadAdminAsync();
    }

    partial void OnCurrentPageChanged(string value)
    {
        OnPropertyChanged(nameof(IsWorkspacePage));
        OnPropertyChanged(nameof(IsReportsPage));
        OnPropertyChanged(nameof(IsUsersPage));
    }

    [RelayCommand]
    private void ToggleSidebar() => IsSidebarCollapsed = !IsSidebarCollapsed;

    [RelayCommand]
    private void NewChat()
    {
        Chat.ClearChat();
        CurrentPage = "workspace";
    }

    [RelayCommand]
    private async Task LoginAsync()
    {
        if (IsLoggingIn)
            return;

        LoginError = null;
        IsLoggingIn = true;
        try
        {
            var session = await _api.LoginAsync(LoginUsername.Trim(), LoginPassword);
            Username = session.User.Username;
            DisplayName = session.User.DisplayName;
            IsAdmin = session.User.Role == AppRoles.Admin;
            IsAuthenticated = true;
            LoginPassword = "";
            CurrentPage = "workspace";
            await Chat.LoadReportsAsync();
            if (IsAdmin)
                await LoadAdminAsync();
        }
        catch (Exception exception)
        {
            LoginError = exception is HttpRequestException
                ? "Неверный логин или пароль, либо API недоступен."
                : $"Не удалось войти: {exception.Message}";
        }
        finally
        {
            IsLoggingIn = false;
        }
    }

    [RelayCommand]
    private void Logout()
    {
        _api.Logout();
        IsAuthenticated = false;
        IsAdmin = false;
        Username = "desktop";
        DisplayName = "Desktop session";
        CurrentPage = "workspace";
        Chat.ClearChat();
        Chat.Reports.Clear();
        Users.Clear();
        PendingRegistrations.Clear();
        LlmSettings = null;
        SelectedLlmProvider = LlmProviders.Local;
        AdminNotice = null;
        AdminError = null;
    }

    [RelayCommand]
    private async Task LoadAdminAsync()
    {
        if (!IsAuthenticated || !IsAdmin)
            return;

        AdminError = null;
        AdminNotice = null;
        try
        {
            Users = new ObservableCollection<AppUserSummary>(await _api.GetUsersAsync());
            PendingRegistrations = new ObservableCollection<RegistrationRequest>(await _api.GetPendingRegistrationsAsync());
            LlmSettings = await _api.GetLlmSettingsAsync();
            SelectedLlmProvider = LlmSettings.Provider;
        }
        catch (Exception exception)
        {
            AdminError = $"Не удалось загрузить админские данные: {exception.Message}";
        }
    }

    [RelayCommand]
    private async Task SaveLlmSettingsAsync()
    {
        if (!IsAuthenticated || !IsAdmin || IsSavingLlmSettings)
            return;

        AdminError = null;
        AdminNotice = null;
        IsSavingLlmSettings = true;
        try
        {
            LlmSettings = await _api.UpdateLlmSettingsAsync(SelectedLlmProvider);
            SelectedLlmProvider = LlmSettings.Provider;
            AdminNotice = $"LLM provider сохранён: {LlmSettings.Provider}.";
        }
        catch (Exception exception)
        {
            AdminError = $"Не удалось сохранить LLM provider: {exception.Message}";
        }
        finally
        {
            IsSavingLlmSettings = false;
        }
    }

    partial void OnLlmSettingsChanged(LlmSettings? value)
    {
        OnPropertyChanged(nameof(CurrentLlmProviderLabel));
        OnPropertyChanged(nameof(LocalLlmLabel));
        OnPropertyChanged(nameof(GigaChatLabel));
        OnPropertyChanged(nameof(GigaChatSecretStatus));
    }

    [RelayCommand]
    private async Task ApproveRegistrationAsync(RegistrationRequest request)
    {
        AdminError = null;
        try
        {
            await _api.ApproveRegistrationAsync(request.Id);
            await LoadAdminAsync();
        }
        catch (Exception exception)
        {
            AdminError = $"Не удалось одобрить заявку: {exception.Message}";
        }
    }

    [RelayCommand]
    private async Task RejectRegistrationAsync(RegistrationRequest request)
    {
        AdminError = null;
        try
        {
            await _api.RejectRegistrationAsync(request.Id, "Отклонено администратором Desktop.");
            await LoadAdminAsync();
        }
        catch (Exception exception)
        {
            AdminError = $"Не удалось отклонить заявку: {exception.Message}";
        }
    }

    [RelayCommand]
    private async Task OpenReportFromListAsync(Report report)
    {
        CurrentPage = "workspace";
        await Chat.OpenReportAsync(report);
    }

    [RelayCommand]
    private async Task DeleteReportFromListAsync(Report report)
    {
        await Chat.DeleteReportAsync(report);
    }

    public async Task InitializeAsync()
    {
        if (IsAuthenticated)
        {
            await Chat.LoadReportsAsync();
            if (IsAdmin)
                await LoadAdminAsync();
        }
    }
}
