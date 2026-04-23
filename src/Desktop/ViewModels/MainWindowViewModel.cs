using CommunityToolkit.Mvvm.ComponentModel;
using CommunityToolkit.Mvvm.Input;
using DriveeDataSpace.DriveeDataSpace.Desktop.Services;

namespace DriveeDataSpace.DriveeDataSpace.Desktop.ViewModels;

public partial class MainWindowViewModel : ObservableObject
{
    public ChatViewModel Chat { get; }

    [ObservableProperty] private bool _isSidebarCollapsed;
    [ObservableProperty] private string _currentPage = "workspace"; // "workspace" | "reports"

    public MainWindowViewModel(BiApiClient api)
    {
        Chat = new ChatViewModel(api);
    }

    [RelayCommand]
    private void NavigateTo(string page) => CurrentPage = page;

    [RelayCommand]
    private void ToggleSidebar() => IsSidebarCollapsed = !IsSidebarCollapsed;

    public async Task InitializeAsync()
    {
        await Chat.LoadReportsAsync();
    }
}