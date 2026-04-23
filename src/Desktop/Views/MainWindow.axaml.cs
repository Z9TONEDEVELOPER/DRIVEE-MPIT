using Avalonia.Controls;
using Avalonia.Input;
using DriveeDataSpace.DriveeDataSpace.Desktop.ViewModels;

namespace DriveeDataSpace.Desktop.Views;

public partial class MainWindow : Window
{
    public MainWindow()
    {
        InitializeComponent();
    }

    private void ChatInput_KeyDown(object? sender, KeyEventArgs e)
    {
        if (e.Key == Key.Enter && e.KeyModifiers == KeyModifiers.None)
        {
            e.Handled = true;
            var vm = DataContext as MainWindowViewModel;
            if (vm?.Chat.SendCommand.CanExecute(null) == true)
                vm.Chat.SendCommand.Execute(null);
        }
        // Shift+Enter — новая строка, обрабатывает TextBox сам
    }
}