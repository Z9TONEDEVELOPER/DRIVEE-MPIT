using DriveeDataSpace.Core.Models;
using DriveeDataSpace.Core.Services;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddOpenApi();
builder.Services.AddHttpClient("llm");
builder.Services.AddSingleton<TenantContext>();
builder.Services.AddSingleton<SecretProtector>();
builder.Services.AddSingleton<AuditLogService>();
builder.Services.AddSingleton<OperationalMetricsService>();
builder.Services.AddSingleton<QueryLoadControl>();
builder.Services.AddSingleton<LoginRateLimitService>();
builder.Services.AddSingleton<LoadCacheService>();
builder.Services.AddSingleton<AnalyticsRegressionService>();
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
builder.Services.AddSingleton<SecurityChecklistService>();
builder.Services.AddSingleton<LlmService>();
builder.Services.AddSingleton<UserService>();
builder.Services.AddSingleton<EmailService>();
builder.Services.AddSingleton<AuthTokenService>();
builder.Services.AddScoped<NlSqlEngine>();
builder.Services.AddSingleton<ReportService>();
builder.Services.AddSingleton<RetentionService>();
builder.Services.AddSingleton<ProductionMetadataGuardService>();
builder.Services.AddSingleton<BackgroundQueryService>();

var app = builder.Build();

app.Services.GetRequiredService<ProductionMetadataGuardService>().EnsureProductionReady();
_ = app.Services.GetRequiredService<DataSourceService>();
app.Services.GetRequiredService<DatasetSeeder>().EnsureSeeded();
_ = app.Services.GetRequiredService<ReportService>();
_ = app.Services.GetRequiredService<UserService>();
_ = app.Services.GetRequiredService<BackgroundQueryService>();
app.Services.GetRequiredService<RetentionService>().Apply(CompanyDefaults.DefaultCompanyId);

if (app.Environment.IsDevelopment())
    app.MapOpenApi();

app.UseHttpsRedirection();

app.MapGet("/health", () => Results.Ok(new
{
    status = "ok",
    service = "DriveeDataSpace.Api"
}));

app.MapPost("/api/auth/login", (LoginRequest request, HttpContext context, UserService users, AuthTokenService tokens, LoginRateLimitService loginRateLimit, AuditLogService audit) =>
{
    if (!loginRateLimit.TryAcquire(request.Username, context.Connection.RemoteIpAddress?.ToString(), out var retryAfter))
    {
        audit.Record(CompanyDefaults.DefaultCompanyId, null, request.Username, "auth.login.rate_limited", "user", success: false, details: BuildRetryAfterMessage(retryAfter));
        return Results.Json(new { error = BuildRetryAfterMessage(retryAfter) }, statusCode: StatusCodes.Status429TooManyRequests);
    }

    var user = users.Authenticate(request.Username, request.Password);
    if (user == null)
    {
        audit.Record(CompanyDefaults.DefaultCompanyId, null, request.Username, "auth.login", "user", success: false);
        return Results.Unauthorized();
    }

    audit.Record(user.CompanyId, user.Id, user.Username, "auth.login", "user", user.Id.ToString(), success: true);
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

    return Results.Ok(users.ListUsers(session.CompanyId));
});

app.MapGet("/api/admin/company", (HttpContext context, AuthTokenService tokens, UserService users) =>
{
    var session = GetCurrentSession(context, tokens);
    if (session == null)
        return Results.Unauthorized();
    if (!IsAdmin(session))
        return Results.Forbid();

    return Results.Ok(users.GetCompany(session.CompanyId));
});

app.MapPost("/api/admin/company", (UpdateCompanyRequest request, HttpContext context, AuthTokenService tokens, UserService users, AuditLogService audit) =>
{
    var session = GetCurrentSession(context, tokens);
    if (session == null)
        return Results.Unauthorized();
    if (!IsAdmin(session))
        return Results.Forbid();

    try
    {
        var company = users.UpdateCompany(session.CompanyId, request.Name);
        audit.Record(session.CompanyId, session.Id, session.Username, "company.update", "company", session.CompanyId.ToString(), success: true, details: company.Name);
        return Results.Ok(company);
    }
    catch (InvalidOperationException exception)
    {
        audit.Record(session.CompanyId, session.Id, session.Username, "company.update", "company", session.CompanyId.ToString(), success: false, details: exception.Message);
        return Results.BadRequest(new { error = exception.Message });
    }
}).DisableAntiforgery();

app.MapPost("/api/admin/users/{id:int}/role", (int id, UpdateUserRoleRequest request, HttpContext context, AuthTokenService tokens, UserService users, AuditLogService audit) =>
{
    var session = GetCurrentSession(context, tokens);
    if (session == null)
        return Results.Unauthorized();
    if (!IsAdmin(session))
        return Results.Forbid();

    try
    {
        var user = users.UpdateUserRole(session.CompanyId, id, request.Role);
        audit.Record(session.CompanyId, session.Id, session.Username, "user.role.update", "user", id.ToString(), success: true, details: user.Role);
        return Results.Ok(user);
    }
    catch (InvalidOperationException exception)
    {
        audit.Record(session.CompanyId, session.Id, session.Username, "user.role.update", "user", id.ToString(), success: false, details: exception.Message);
        return Results.BadRequest(new { error = exception.Message });
    }
}).DisableAntiforgery();

app.MapPost("/api/admin/users/{id:int}/active", (int id, UpdateUserActiveRequest request, HttpContext context, AuthTokenService tokens, UserService users, AuditLogService audit) =>
{
    var session = GetCurrentSession(context, tokens);
    if (session == null)
        return Results.Unauthorized();
    if (!IsAdmin(session))
        return Results.Forbid();
    if (session.Id == id && !request.IsActive)
        return Results.BadRequest(new { error = "Нельзя отключить текущий сеанс администратора." });

    try
    {
        var user = users.SetUserActive(session.CompanyId, id, request.IsActive);
        audit.Record(session.CompanyId, session.Id, session.Username, request.IsActive ? "user.activate" : "user.deactivate", "user", id.ToString(), success: true);
        return Results.Ok(user);
    }
    catch (InvalidOperationException exception)
    {
        audit.Record(session.CompanyId, session.Id, session.Username, request.IsActive ? "user.activate" : "user.deactivate", "user", id.ToString(), success: false, details: exception.Message);
        return Results.BadRequest(new { error = exception.Message });
    }
}).DisableAntiforgery();

app.MapGet("/api/admin/registration-requests", (string? status, HttpContext context, AuthTokenService tokens, UserService users) =>
{
    var session = GetCurrentSession(context, tokens);
    if (session == null)
        return Results.Unauthorized();
    if (!IsAdmin(session))
        return Results.Forbid();

    return Results.Ok(users.ListRegistrationRequests(session.CompanyId, status));
});

app.MapGet("/api/admin/llm-settings", (HttpContext context, AuthTokenService tokens, LlmSettingsService settings) =>
{
    var session = GetCurrentSession(context, tokens);
    if (session == null)
        return Results.Unauthorized();
    if (!IsAdmin(session))
        return Results.Forbid();

    return Results.Ok(settings.Get(session.CompanyId));
});

app.MapGet("/api/admin/security-checklist", (HttpContext context, AuthTokenService tokens, SecurityChecklistService checklist) =>
{
    var session = GetCurrentSession(context, tokens);
    if (session == null)
        return Results.Unauthorized();
    if (!IsAdmin(session))
        return Results.Forbid();

    return Results.Ok(checklist.Get(session.CompanyId));
});

app.MapPost("/api/admin/llm-settings", (UpdateLlmSettingsRequest request, HttpContext context, AuthTokenService tokens, LlmSettingsService settings, AuditLogService audit) =>
{
    var session = GetCurrentSession(context, tokens);
    if (session == null)
        return Results.Unauthorized();
    if (!IsAdmin(session))
        return Results.Forbid();

    try
    {
        var result = settings.Save(request, session.CompanyId);
        audit.Record(session.CompanyId, session.Id, session.Username, "llm_settings.update", "llm_settings", success: true, details: result.Provider);
        return Results.Ok(result);
    }
    catch (InvalidOperationException exception)
    {
        audit.Record(session.CompanyId, session.Id, session.Username, "llm_settings.update", "llm_settings", success: false, details: exception.Message);
        return Results.BadRequest(new { error = exception.Message });
    }
}).DisableAntiforgery();

app.MapPost("/api/admin/data-sources/rotate-secrets", (HttpContext context, AuthTokenService tokens, DataSourceService dataSources, AuditLogService audit) =>
{
    var session = GetCurrentSession(context, tokens);
    if (session == null)
        return Results.Unauthorized();
    if (!IsAdmin(session))
        return Results.Forbid();

    try
    {
        var result = dataSources.RotateConnectionSecrets(session.CompanyId);
        audit.Record(session.CompanyId, session.Id, session.Username, "data_source.secret.rotate", "data_source", success: true, details: $"{result.RotatedCount} rotated, {result.SkippedCount} skipped");
        return Results.Ok(result);
    }
    catch (InvalidOperationException exception)
    {
        audit.Record(session.CompanyId, session.Id, session.Username, "data_source.secret.rotate", "data_source", success: false, details: exception.Message);
        return Results.BadRequest(new { error = exception.Message });
    }
}).DisableAntiforgery();

app.MapGet("/api/admin/analytics-regression", async (
    HttpContext context,
    AuthTokenService tokens,
    TenantContext tenantContext,
    AnalyticsRegressionService regression,
    AuditLogService audit,
    CancellationToken cancellationToken) =>
{
    var session = GetCurrentSession(context, tokens);
    if (session == null)
        return Results.Unauthorized();
    if (!IsAdmin(session))
        return Results.Forbid();

    using var tenantScope = tenantContext.Use(session.CompanyId);
    var result = await regression.RunAsync(session.CompanyId, cancellationToken);
    audit.Record(session.CompanyId, session.Id, session.Username, "analytics_regression.run", "analytics_regression", success: result.Passed, details: $"{result.PassedCount}/{result.Cases.Count}");
    return Results.Ok(result);
});

app.MapGet("/api/admin/metrics", (
    HttpContext context,
    AuthTokenService tokens,
    OperationalMetricsService metrics,
    AuditLogService audit) =>
{
    var session = GetCurrentSession(context, tokens);
    if (session == null)
        return Results.Unauthorized();
    if (!IsAdmin(session))
        return Results.Forbid();

    return Results.Ok(new
    {
        metrics = metrics.GetSnapshot(session.CompanyId),
        audit = audit.GetSummary(session.CompanyId)
    });
});

app.MapPost("/api/admin/retention/run", (
    HttpContext context,
    AuthTokenService tokens,
    RetentionService retention,
    AuditLogService audit) =>
{
    var session = GetCurrentSession(context, tokens);
    if (session == null)
        return Results.Unauthorized();
    if (!IsAdmin(session))
        return Results.Forbid();

    var result = retention.Apply(session.CompanyId);
    audit.Record(session.CompanyId, session.Id, session.Username, "retention.run", "retention", success: true, details: $"audit={result.DeletedAuditEvents}; reports={result.DeletedReports}");
    return Results.Ok(result);
}).DisableAntiforgery();

app.MapGet("/api/admin/audit-log", (
    string? action,
    bool? errorsOnly,
    int? limit,
    HttpContext context,
    AuthTokenService tokens,
    AuditLogService audit) =>
{
    var session = GetCurrentSession(context, tokens);
    if (session == null)
        return Results.Unauthorized();
    if (!IsAdmin(session))
        return Results.Forbid();

    return Results.Ok(audit.List(
        session.CompanyId,
        limit ?? 200,
        action,
        errorsOnly == true));
});

app.MapPost("/api/admin/registration-requests/{id:int}/approve", async (
    int id,
    HttpContext context,
    AuthTokenService tokens,
    UserService users,
    EmailService email,
    AuditLogService audit,
    CancellationToken cancellationToken) =>
{
    var session = GetCurrentSession(context, tokens);
    if (session == null)
        return Results.Unauthorized();
    if (!IsAdmin(session))
        return Results.Forbid();

    try
    {
        var result = users.ApproveRegistrationRequest(session.CompanyId, id, session.Id);
        await email.SendRegistrationApprovedAsync(result.Request, cancellationToken);
        audit.Record(session.CompanyId, session.Id, session.Username, "registration.approve", "registration_request", id.ToString(), success: true);
        return Results.Ok(result);
    }
    catch (InvalidOperationException exception)
    {
        audit.Record(session.CompanyId, session.Id, session.Username, "registration.approve", "registration_request", id.ToString(), success: false, details: exception.Message);
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
    AuditLogService audit,
    CancellationToken cancellationToken) =>
{
    var session = GetCurrentSession(context, tokens);
    if (session == null)
        return Results.Unauthorized();
    if (!IsAdmin(session))
        return Results.Forbid();

    try
    {
        var result = users.RejectRegistrationRequest(session.CompanyId, id, session.Id, request.Reason);
        await email.SendRegistrationRejectedAsync(result.Request, cancellationToken);
        audit.Record(session.CompanyId, session.Id, session.Username, "registration.reject", "registration_request", id.ToString(), success: true);
        return Results.Ok(result);
    }
    catch (InvalidOperationException exception)
    {
        audit.Record(session.CompanyId, session.Id, session.Username, "registration.reject", "registration_request", id.ToString(), success: false, details: exception.Message);
        return Results.BadRequest(new { error = exception.Message });
    }
}).DisableAntiforgery();

app.MapPost("/api/query", async (
    QueryRequest request,
    HttpContext context,
    AuthTokenService tokens,
    NlSqlEngine engine,
    TenantContext tenantContext,
    AuditLogService audit,
    CancellationToken cancellationToken) =>
{
    var session = GetCurrentSession(context, tokens);
    if (session == null)
        return Results.Unauthorized();

    if (string.IsNullOrWhiteSpace(request.Text))
        return Results.BadRequest(new { error = "Query text is empty." });

    using var tenantScope = tenantContext.Use(session.CompanyId);
    var queryText = request.Text.Trim();
    var result = await engine.RunAsync(queryText, request.History, request.PreviousIntent, cancellationToken, userKey: session.Username);
    result.UserQuery = queryText;
    audit.Record(session.CompanyId, session.Id, session.Username, "query.run", "query", success: string.IsNullOrWhiteSpace(result.Error), details: queryText);
    return Results.Ok(result);
}).DisableAntiforgery();

app.MapPost("/api/query/jobs", async (
    QueryJobSubmitRequest request,
    HttpContext context,
    AuthTokenService tokens,
    BackgroundQueryService jobs,
    AuditLogService audit,
    CancellationToken cancellationToken) =>
{
    var session = GetCurrentSession(context, tokens);
    if (session == null)
        return Results.Unauthorized();

    try
    {
        var job = await jobs.EnqueueAsync(request, session.CompanyId, session.Id, session.Username, cancellationToken);
        audit.Record(session.CompanyId, session.Id, session.Username, "query.background.enqueue", "query_job", job.Id, success: true, details: request.Text);
        return Results.Accepted($"/api/query/jobs/{job.Id}", job);
    }
    catch (InvalidOperationException exception)
    {
        audit.Record(session.CompanyId, session.Id, session.Username, "query.background.enqueue", "query_job", success: false, details: exception.Message);
        return Results.BadRequest(new { error = exception.Message });
    }
}).DisableAntiforgery();

app.MapGet("/api/query/jobs/{id}", (
    string id,
    HttpContext context,
    AuthTokenService tokens,
    BackgroundQueryService jobs) =>
{
    var session = GetCurrentSession(context, tokens);
    if (session == null)
        return Results.Unauthorized();

    var job = jobs.Get(id, session.CompanyId, session.Username, IsAdmin(session));
    return job == null ? Results.NotFound(new { error = "Query job not found." }) : Results.Ok(job);
});

app.MapDelete("/api/query/jobs/{id}", (
    string id,
    HttpContext context,
    AuthTokenService tokens,
    BackgroundQueryService jobs,
    AuditLogService audit) =>
{
    var session = GetCurrentSession(context, tokens);
    if (session == null)
        return Results.Unauthorized();

    var canceled = jobs.Cancel(id, session.CompanyId, session.Username, IsAdmin(session));
    if (!canceled)
        return Results.NotFound(new { error = "Query job not found." });

    audit.Record(session.CompanyId, session.Id, session.Username, "query.background.cancel", "query_job", id, success: true);
    return Results.NoContent();
});

app.MapGet("/api/reports", (HttpContext context, AuthTokenService tokens, ReportService reports) =>
{
    var session = GetCurrentSession(context, tokens);
    return session == null
        ? Results.Unauthorized()
        : Results.Ok(reports.ListForAuthor(session.CompanyId, session.Username));
});

app.MapPost("/api/reports", (SaveReportRequest request, HttpContext context, AuthTokenService tokens, ReportService reports, AuditLogService audit) =>
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
        CompanyId = session.CompanyId,
        UserQuery = request.UserQuery?.Trim() ?? string.Empty,
        IntentJson = request.IntentJson,
        Sql = request.Sql,
        Visualization = string.IsNullOrWhiteSpace(request.Visualization) ? "table" : request.Visualization.Trim(),
        Author = session.Username,
        CreatedAt = DateTime.UtcNow
    };

    report.Id = reports.Save(report);
    audit.Record(session.CompanyId, session.Id, session.Username, "report.save", "report", report.Id.ToString(), success: true);
    return Results.Ok(report);
}).DisableAntiforgery();

app.MapPost("/api/reports/{id:int}/rerun", (int id, HttpContext context, AuthTokenService tokens, NlSqlEngine engine, ReportService reports, TenantContext tenantContext, AuditLogService audit) =>
{
    var session = GetCurrentSession(context, tokens);
    if (session == null)
        return Results.Unauthorized();

    var report = reports.GetForAuthor(session.CompanyId, id, session.Username, IsAdmin(session));
    if (report == null)
        return Results.NotFound(new { error = "Report not found." });

    using var tenantScope = tenantContext.Use(session.CompanyId);
    var result = engine.ReplayFromReport(report.IntentJson, userKey: session.Username);
    result.UserQuery = report.UserQuery;
    audit.Record(session.CompanyId, session.Id, session.Username, "report.rerun", "report", id.ToString(), success: true);
    return Results.Ok(result);
}).DisableAntiforgery();

app.MapDelete("/api/reports/{id:int}", (int id, HttpContext context, AuthTokenService tokens, ReportService reports, AuditLogService audit) =>
{
    var session = GetCurrentSession(context, tokens);
    if (session == null)
        return Results.Unauthorized();

    reports.DeleteForAuthor(session.CompanyId, id, session.Username, IsAdmin(session));
    audit.Record(session.CompanyId, session.Id, session.Username, "report.delete", "report", id.ToString(), success: true);
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
    AppRoles.CanAdminister(session.Role);

static string BuildRetryAfterMessage(TimeSpan retryAfter) =>
    $"Too many login attempts. Try again in {Math.Max(1, (int)Math.Ceiling(retryAfter.TotalSeconds))} sec.";
