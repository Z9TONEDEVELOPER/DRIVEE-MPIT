using Avalonia;
using Avalonia.Controls.ApplicationLifetimes;
using DriveeDataSpace.Desktop.Services;
using DriveeDataSpace.Desktop.ViewModels;
using DriveeDataSpace.Desktop.Views;
using Microsoft.Extensions.DependencyInjection;
using Avalonia.Markup.Xaml;
using System;
namespace DriveeDataSpace.Desktop;

public partial class App : Application
{
    public override void Initialize() => AvaloniaXamlLoader.Load(this);

    public override void OnFrameworkInitializationCompleted()
    {
        var services = new ServiceCollection();
        services.AddHttpClient<DesktopApiClient>(client =>
        {
            client.BaseAddress = new Uri("http://localhost:5000"); // Адрес DriveeBI.Api
            client.Timeout = TimeSpan.FromSeconds(30);
        });
        services.AddSingleton<MainWindowViewModel>();

        var provider = services.BuildServiceProvider();

        if (ApplicationLifetime is IClassicDesktopStyleApplicationLifetime desktop)
        {
            desktop.MainWindow = new MainWindow
            {
                DataContext = provider.GetRequiredService<MainWindowViewModel>()
            };
            desktop.MainWindow.Loaded += (_, _) => 
                ((MainWindowViewModel)desktop.MainWindow.DataContext!).LoadInitialData();
        }

        base.OnFrameworkInitializationCompleted();
    }
}