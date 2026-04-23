using System;
using Avalonia;
using Avalonia.Controls.ApplicationLifetimes;
using Avalonia.Markup.Xaml;
using DriveeDataSpace.DriveeDataSpace.Desktop.Services;
using DriveeDataSpace.DriveeDataSpace.Desktop.ViewModels;
using DriveeDataSpace.Desktop.Views;
using Microsoft.Extensions.DependencyInjection;

namespace DriveeDataSpace.Desktop;

public partial class App : Application
{
    public override void Initialize() => AvaloniaXamlLoader.Load(this);

    public override void OnFrameworkInitializationCompleted()
    {
        // ── DI ───────────────────────────────────────────────────────────────
        var services = new ServiceCollection();

        services.AddHttpClient<BiApiClient>(client =>
        {
            // URL Web-бэкенда. В prod можно брать из конфига/env.
            client.BaseAddress = new Uri(
                Environment.GetEnvironmentVariable("DRIVEE_API_URL")
                ?? "http://localhost:5012");
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