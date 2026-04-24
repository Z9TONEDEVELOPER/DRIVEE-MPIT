using System.Collections.Specialized;
using System.ComponentModel;
using Avalonia;
using Avalonia.Controls;
using Avalonia.Input;
using Avalonia.Threading;
using DriveeDataSpace.DriveeDataSpace.Desktop.ViewModels;

namespace DriveeDataSpace.Desktop.Views;

public partial class MainWindow : Window
{
    private MainWindowViewModel? _wiredViewModel;

    public MainWindow()
    {
        InitializeComponent();
        DataContextChanged += (_, _) => WireViewModel();
    }

    private void WireViewModel()
    {
        if (DataContext is not MainWindowViewModel vm)
            return;

        if (ReferenceEquals(_wiredViewModel, vm))
            return;

        if (_wiredViewModel != null)
        {
            _wiredViewModel.Chat.Messages.CollectionChanged -= OnMessagesChanged;
            _wiredViewModel.Chat.PropertyChanged -= OnChatPropertyChanged;
        }

        _wiredViewModel = vm;
        vm.Chat.Messages.CollectionChanged += OnMessagesChanged;
        vm.Chat.PropertyChanged += OnChatPropertyChanged;
    }

    private void OnMessagesChanged(object? sender, NotifyCollectionChangedEventArgs e)
    {
        ScrollChatToBottom();
    }

    private void OnChatPropertyChanged(object? sender, PropertyChangedEventArgs e)
    {
        if (e.PropertyName == nameof(ChatViewModel.IsBusy))
            ScrollChatToBottom();
    }

    private void ScrollChatToBottom()
    {
        Dispatcher.UIThread.Post(() =>
        {
            ChatScrollViewer.Offset = new Vector(
                ChatScrollViewer.Offset.X,
                ChatScrollViewer.Extent.Height);
        }, DispatcherPriority.Background);
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

    private void LoginPassword_KeyDown(object? sender, KeyEventArgs e)
    {
        if (e.Key == Key.Enter && e.KeyModifiers == KeyModifiers.None)
        {
            e.Handled = true;
            if (DataContext is MainWindowViewModel vm && vm.LoginCommand.CanExecute(null))
                vm.LoginCommand.Execute(null);
        }
    }
}
