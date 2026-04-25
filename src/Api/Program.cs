using DriveeDataSpace.Core.Models;
using DriveeDataSpace.Core.Services;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddOpenApi();
builder.Services.AddHttpClient("llm");
builder.Services.AddSingleton<DataSourceService>();
builder.Services.AddSingleton<SemanticLayer>();
builder.Services.AddSingleton<DatasetSeeder>();
builder.Services.AddSingleton<AnalyticsTimeService>();
builder.Services.AddSingleton<DateResolver>();
builder.Services.AddSingleton<IntentValidator>();
builder.Services.AddSingleton<SqlBuilder>();
builder.Services.AddSingleton<SqlGuard>();
builder.Services.AddSingleton<QueryExecutor>();
builder.Services.AddSingleton<ExplainEngine>();
builder.Services.AddSingleton<LlmSettingsService>();
builder.Services.AddSingleton<LlmService>();
builder.Services.AddSingleton<UserService>();
builder.Services.AddSingleton<EmailService>();
builder.Services.AddSingleton<AuthTokenService>();
builder.Services.AddScoped<NlSqlEngine>();
builder.Services.AddSingleton<ReportService>();

var app = builder.Build();

_ = app.Services.GetRequiredService<DataSourceService>();
app.Services.GetRequiredService<DatasetSeeder>().EnsureSeeded();
_ = app.Services.GetRequiredService<ReportService>();
_ = app.Services.GetRequiredService<UserService>();

if (app.Environment.IsDevelopment())
    app.MapOpenApi();

app.UseHttpsRedirection();

app.MapGet("/health", () => Results.Ok(new
{
    status = "ok",
    service = "DriveeDataSpace.Api"
}));

app.MapPost("/api/auth/login", (LoginRequest request, UserService users, AuthTokenService tokens) =>
{
    var user = users.Authenticate(request.Username, request.Password);
    if (user == null)
        return Results.Unauthorized();

    return Results.Ok(tokens.CreateSession(user));
}).DisableAntiforgery();

app.MapGet("/api/auth/me", (HttpContext context, AuthTokenService tokens, UserService users) =>
{
    var session = GetCurrentSession(context, tokens);
    if (session == null)
        return Results.Unauthorized();

    var user = users.FindById(session.Id);
    return user == null || !user.IsActive ? Results.Unauthorized() : Results.Ok(user);
});

app.MapPost("/api/registration-requests", (RegistrationRequestInput request, UserService users) =>
{
    try
    {
        return Results.Ok(users.SubmitRegistrationRequest(request));
    }
    catch (InvalidOperationException exception)
    {
        return Results.BadRequest(new { error = exception.Message });
    }
}).DisableAntiforgery();

app.MapGet("/api/admin/users", (HttpContext context, AuthTokenService tokens, UserService users) =>
{
    var session = GetCurrentSession(context, tokens);
    if (session == null)
        return Results.Unauthorized();
    if (!IsAdmin(session))
        return Results.Forbid();

    return Results.Ok(users.ListUsers());
});

app.MapGet("/api/admin/registration-requests", (string? status, HttpContext context, AuthTokenService tokens, UserService users) =>
{
    var session = GetCurrentSession(context, tokens);
    if (session == null)
        return Results.Unauthorized();
    if (!IsAdmin(session))
        return Results.Forbid();

    return Results.Ok(users.ListRegistrationRequests(status));
});

app.MapGet("/api/admin/llm-settings", (HttpContext context, AuthTokenService tokens, LlmSettingsService settings) =>
{
    var session = GetCurrentSession(context, tokens);
    if (session == null)
        return Results.Unauthorized();
    if (!IsAdmin(session))
        return Results.Forbid();

    return Results.Ok(settings.Get());
});

app.MapPost("/api/admin/llm-settings", (UpdateLlmSettingsRequest request, HttpContext context, AuthTokenService tokens, LlmSettingsService settings) =>
{
    var session = GetCurrentSession(context, tokens);
    if (session == null)
        return Results.Unauthorized();
    if (!IsAdmin(session))
        return Results.Forbid();

    try
    {
        return Results.Ok(settings.SetProvider(request.Provider));
    }
    catch (InvalidOperationException exception)
    {
        return Results.BadRequest(new { error = exception.Message });
    }
}).DisableAntiforgery();

app.MapPost("/api/admin/registration-requests/{id:int}/approve", async (
    int id,
    HttpContext context,
    AuthTokenService tokens,
    UserService users,
    EmailService email,
    CancellationToken cancellationToken) =>
{
    var session = GetCurrentSession(context, tokens);
    if (session == null)
        return Results.Unauthorized();
    if (!IsAdmin(session))
        return Results.Forbid();

    try
    {
        var result = users.ApproveRegistrationRequest(id, session.Id);
        await email.SendRegistrationApprovedAsync(result.Request, cancellationToken);
        return Results.Ok(result);
    }
    catch (InvalidOperationException exception)
    {
        return Results.BadRequest(new { error = exception.Message });
    }
}).DisableAntiforgery();

app.MapPost("/api/admin/registration-requests/{id:int}/reject", async (
    int id,
    RejectRegistrationRequest request,
    HttpContext context,
    AuthTokenService tokens,
    UserService users,
    EmailService email,
    CancellationToken cancellationToken) =>
{
    var session = GetCurrentSession(context, tokens);
    if (session == null)
        return Results.Unauthorized();
    if (!IsAdmin(session))
        return Results.Forbid();

    try
    {
        var result = users.RejectRegistrationRequest(id, session.Id, request.Reason);
        await email.SendRegistrationRejectedAsync(result.Request, cancellationToken);
        return Results.Ok(result);
    }
    catch (InvalidOperationException exception)
    {
        return Results.BadRequest(new { error = exception.Message });
    }
}).DisableAntiforgery();

app.MapPost("/api/query", async (
    QueryRequest request,
    HttpContext context,
    AuthTokenService tokens,
    NlSqlEngine engine,
    CancellationToken cancellationToken) =>
{
    if (GetCurrentSession(context, tokens) == null)
        return Results.Unauthorized();

    if (string.IsNullOrWhiteSpace(request.Text))
        return Results.BadRequest(new { error = "Query text is empty." });

    var result = await engine.RunAsync(
        request.Text.Trim(),
        request.History,
        request.PreviousIntent,
        cancellationToken);
    result.UserQuery = request.Text.Trim();
    return Results.Ok(result);
}).DisableAntiforgery();

app.MapGet("/api/reports", (HttpContext context, AuthTokenService tokens, ReportService reports) =>
{
    var session = GetCurrentSession(context, tokens);
    return session == null
        ? Results.Unauthorized()
        : Results.Ok(reports.ListForAuthor(session.Username));
});

app.MapPost("/api/reports", (SaveReportRequest request, HttpContext context, AuthTokenService tokens, ReportService reports) =>
{
    var session = GetCurrentSession(context, tokens);
    if (session == null)
        return Results.Unauthorized();

    if (string.IsNullOrWhiteSpace(request.Name))
        return Results.BadRequest(new { error = "Report name is empty." });
    if (string.IsNullOrWhiteSpace(request.IntentJson) || string.IsNullOrWhiteSpace(request.Sql))
        return Results.BadRequest(new { error = "Report payload is incomplete." });

    var report = new Report
    {
        Name = request.Name.Trim(),
        UserQuery = request.UserQuery?.Trim() ?? string.Empty,
        IntentJson = request.IntentJson,
        Sql = request.Sql,
        Visualization = string.IsNullOrWhiteSpace(request.Visualization) ? "table" : request.Visualization.Trim(),
        Author = session.Username,
        CreatedAt = DateTime.UtcNow
    };

    report.Id = reports.Save(report);
    return Results.Ok(report);
}).DisableAntiforgery();

app.MapPost("/api/reports/{id:int}/rerun", (int id, HttpContext context, AuthTokenService tokens, NlSqlEngine engine, ReportService reports) =>
{
    var session = GetCurrentSession(context, tokens);
    if (session == null)
        return Results.Unauthorized();

    var report = reports.GetForAuthor(id, session.Username, IsAdmin(session));
    if (report == null)
        return Results.NotFound(new { error = "Report not found." });

    var result = engine.ReplayFromReport(report.IntentJson);
    result.UserQuery = report.UserQuery;
    return Results.Ok(result);
}).DisableAntiforgery();

app.MapDelete("/api/reports/{id:int}", (int id, HttpContext context, AuthTokenService tokens, ReportService reports) =>
{
    var session = GetCurrentSession(context, tokens);
    if (session == null)
        return Results.Unauthorized();

    reports.DeleteForAuthor(id, session.Username, IsAdmin(session));
    return Results.NoContent();
});

app.Run();

static AuthUserSession? GetCurrentSession(HttpContext context, AuthTokenService tokens)
{
    var authorization = context.Request.Headers.Authorization.ToString();
    const string bearerPrefix = "Bearer ";
    return authorization.StartsWith(bearerPrefix, StringComparison.OrdinalIgnoreCase)
        ? tokens.Validate(authorization[bearerPrefix.Length..].Trim())
        : null;
}

static bool IsAdmin(AuthUserSession session) =>
    string.Equals(session.Role, AppRoles.Admin, StringComparison.OrdinalIgnoreCase);
