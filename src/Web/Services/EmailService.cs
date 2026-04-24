using DriveeDataSpace.Web.Models;

namespace DriveeDataSpace.Web.Services;

public sealed class EmailService
{
    private readonly ILogger<EmailService> _logger;

    public EmailService(ILogger<EmailService> logger)
    {
        _logger = logger;
    }

    public Task SendRegistrationApprovedAsync(RegistrationRequest request, CancellationToken cancellationToken = default)
    {
        LogLocalEmail(
            request.Email,
            "Drivee BI: доступ одобрен",
            $"""
            Здравствуйте, {request.DisplayName}!

            Ваш запрос на доступ к Drivee BI одобрен.
            Для входа используйте email: {request.Email}
            Пароль тот же, который вы указали при регистрации.
            """);

        return Task.CompletedTask;
    }

    public Task SendRegistrationRejectedAsync(RegistrationRequest request, CancellationToken cancellationToken = default)
    {
        var reason = string.IsNullOrWhiteSpace(request.RejectionReason)
            ? "Причина не указана."
            : request.RejectionReason;

        LogLocalEmail(
            request.Email,
            "Drivee BI: запрос на доступ отклонён",
            $"""
            Здравствуйте, {request.DisplayName}!

            Ваш запрос на доступ к Drivee BI отклонён.
            Причина: {reason}
            """);

        return Task.CompletedTask;
    }

    private void LogLocalEmail(string to, string subject, string body)
    {
        _logger.LogInformation(
            "LOCAL EMAIL\nTo: {To}\nSubject: {Subject}\n\n{Body}",
            to,
            subject,
            body);
    }
}
