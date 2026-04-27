using System;
using Avalonia;
using Avalonia.Controls.ApplicationLifetimes;
using Avalonia.Markup.Xaml;
using NexusDataSpace.Desktop.Services;
using NexusDataSpace.Desktop.ViewModels;
using NexusDataSpace.Desktop.Views;
using Microsoft.Extensions.DependencyInjection;

namespace NexusDataSpace.Desktop;

public partial class App : Application
{
    public override void Initialize() => AvaloniaXamlLoader.Load(this);

    public override void OnFrameworkInitializationCompleted()
    {
        // ── DI ───────────────────────────────────────────────────────────────
        var services = new ServiceCollection();

        services.AddHttpClient<BiApiClient>(client =>
        {
            // URL инфраструктурного API. Web может работать отдельно как Blazor UI.
            client.BaseAddress = new Uri(
                Environment.GetEnvironmentVariable("NEXUS_DATA_SPACE_API_URL")
                ?? "http://localhost:5099");
            client.Timeout = TimeSpan.FromSeconds(120);
        });

        services.AddSingleton<MainWindowViewModel>();

        var provider = services.BuildServiceProvider();

        // ── Window ───────────────────────────────────────────────────────────
        if (ApplicationLifetime is IClassicDesktopStyleApplicationLifetime desktop)
        {
            var vm = provider.GetRequiredService<MainWindowViewModel>();
            var win = new MainWindow { DataContext = vm };

            win.Loaded += async (_, _) => await vm.InitializeAsync();

            desktop.MainWindow = win;
        }

        base.OnFrameworkInitializationCompleted();
    }
}
