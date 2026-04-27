using System.Security.Claims;
using NexusDataSpace.Web.Components;
using NexusDataSpace.Core.Models;
using NexusDataSpace.Core.Services;
using NexusDataSpace.Web.Services;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authentication.Cookies;

var builder = WebApplication.CreateBuilder(args);

builder.Logging.ClearProviders();
builder.Logging.AddConsole();
builder.Logging.AddDebug();

builder.Services.AddRazorComponents()
    .AddInteractiveServerComponents();
builder.Services.AddCascadingAuthenticationState();
builder.Services.AddAuthentication(CookieAuthenticationDefaults.AuthenticationScheme)
    .AddCookie(options =>
    {
        options.Cookie.Name = builder.Configuration["Auth:CookieName"] ?? "nexus.data.space.auth";
        options.Cookie.HttpOnly = true;
        options.Cookie.SameSite = SameSiteMode.Strict;
        options.Cookie.SecurePolicy = CookieSecurePolicy.SameAsRequest;
        options.LoginPath = "/login";
        options.AccessDeniedPath = "/login";
        options.SlidingExpiration = true;
        options.ExpireTimeSpan = TimeSpan.FromHours(12);
    });
builder.Services.AddAuthorization();
builder.Services.AddHttpContextAccessor();

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
builder.Services.AddSingleton<AuthTokenService>();
builder.Services.AddSingleton<EmailService>();
builder.Services.AddScoped<NlSqlEngine>();
builder.Services.AddScoped<WorkspaceSessionState>();
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

if (!app.Environment.IsDevelopment())
{
    app.UseExceptionHandler("/Error", createScopeForErrors: true);
    app.UseHsts();
}

app.UseHttpsRedirection();
app.UseAuthentication();
app.Use(async (context, next) =>
{
    if (context.User.Identity?.IsAuthenticated != true)
    {
        var authorization = context.Request.Headers.Authorization.ToString();
        if (authorization.StartsWith("Bearer ", StringComparison.OrdinalIgnoreCase))
        {
            var token = authorization["Bearer ".Length..].Trim();
            var session = context.RequestServices.GetRequiredService<AuthTokenService>().Validate(token);
            if (session != null)
            {
                var claims = new List<Claim>
                {
                    new(ClaimTypes.NameIdentifier, session.Id.ToString()),
                    new("company_id", session.CompanyId.ToString()),
                    new(ClaimTypes.Name, session.Username),
                    new(ClaimTypes.GivenName, session.DisplayName),
                    new(ClaimTypes.Role, session.Role)
                };
                context.User = new ClaimsPrincipal(new ClaimsIdentity(claims, "Bearer"));
            }
        }
    }

    await next();
});
app.UseAuthorization();
app.UseAntiforgery();

app.MapPost("/auth/login", async (HttpContext context, UserService users, LoginRateLimitService loginRateLimit, AuditLogService audit) =>
{
    var form = await context.Request.ReadFormAsync();
    var username = form["username"].ToString();
    var password = form["password"].ToString();
    var returnUrl = NormalizeReturnUrl(form["returnUrl"].ToString());

    if (!loginRateLimit.TryAcquire(username, context.Connection.RemoteIpAddress?.ToString(), out var retryAfter))
    {
        var message = BuildRetryAfterMessage(retryAfter);
        audit.Record(CompanyDefaults.DefaultCompanyId, null, username, "auth.login.rate_limited", "user", success: false, details: message);
        var rateLimitedUrl = $"/login?error={Uri.EscapeDataString(message)}&returnUrl={Uri.EscapeDataString(returnUrl)}";
        context.Response.Redirect(rateLimitedUrl);
        return;
    }

    var user = users.Authenticate(username, password);
    if (user == null)
    {
        audit.Record(CompanyDefaults.DefaultCompanyId, null, username, "auth.login", "user", success: false);
        var loginUrl = $"/login?error={Uri.EscapeDataString("Неверный логин или пароль.")}&returnUrl={Uri.EscapeDataString(returnUrl)}";
        context.Response.Redirect(loginUrl);
        return;
    }

    var claims = new List<Claim>
    {
        new(ClaimTypes.NameIdentifier, user.Id.ToString()),
        new("company_id", user.CompanyId.ToString()),
        new(ClaimTypes.Name, user.Username),
        new(ClaimTypes.GivenName, user.DisplayName),
        new(ClaimTypes.Role, user.Role)
    };

    var identity = new ClaimsIdentity(claims, CookieAuthenticationDefaults.AuthenticationScheme);
    var principal = new ClaimsPrincipal(identity);

    await context.SignInAsync(
        CookieAuthenticationDefaults.AuthenticationScheme,
        principal,
        new AuthenticationProperties
        {
            IsPersistent = true,
            AllowRefresh = true,
            ExpiresUtc = DateTimeOffset.UtcNow.AddHours(12)
        });

    audit.Record(user.CompanyId, user.Id, user.Username, "auth.login", "user", user.Id.ToString(), success: true);
    context.Response.Redirect(returnUrl);
}).DisableAntiforgery().AllowAnonymous();

app.MapPost("/auth/logout", async (HttpContext context) =>
{
    await context.SignOutAsync(CookieAuthenticationDefaults.AuthenticationScheme);
    context.Response.Redirect("/login");
}).DisableAntiforgery().AllowAnonymous();

app.MapPost("/api/auth/login", (
    LoginRequest request,
    UserService users,
    AuthTokenService tokens,
    LoginRateLimitService loginRateLimit,
    AuditLogService audit,
    HttpContext context) =>
{
    var username = request.Username?.Trim() ?? "";
    if (!loginRateLimit.TryAcquire(username, context.Connection.RemoteIpAddress?.ToString(), out var retryAfter))
    {
        var message = BuildRetryAfterMessage(retryAfter);
        audit.Record(CompanyDefaults.DefaultCompanyId, null, username, "auth.api_login.rate_limited", "user", success: false, details: message);
        return Results.Json(new { error = message }, statusCode: StatusCodes.Status429TooManyRequests);
    }

    var user = users.Authenticate(username, request.Password);
    if (user == null)
    {
        audit.Record(CompanyDefaults.DefaultCompanyId, null, username, "auth.api_login", "user", success: false);
        return Results.Unauthorized();
    }

    var session = tokens.CreateSession(user);
    audit.Record(user.CompanyId, user.Id, user.Username, "auth.api_login", "user", user.Id.ToString(), success: true);
    return Results.Ok(session);
}).DisableAntiforgery().AllowAnonymous();

app.MapGet("/api/admin/users", (UserService users, HttpContext context) =>
{
    if (!IsApiAdmin(context))
        return Results.Forbid();

    return Results.Ok(users.ListUsers(GetApiCompanyId(context)));
});

app.MapGet("/api/admin/company", (UserService users, HttpContext context) =>
{
    if (!IsApiAdmin(context))
        return Results.Forbid();

    return Results.Ok(users.GetCompany(GetApiCompanyId(context)));
});

app.MapPost("/api/admin/company", (UpdateCompanyRequest request, UserService users, AuditLogService audit, HttpContext context) =>
{
    if (!IsApiAdmin(context))
        return Results.Forbid();

    var company = users.UpdateCompany(GetApiCompanyId(context), request.Name);
    audit.Record(company.Id, GetApiUserId(context), GetApiUserName(context), "company.update", "company", company.Id.ToString(), success: true, details: company.Name);
    return Results.Ok(company);
}).DisableAntiforgery();

app.MapGet("/api/admin/registration-requests", (string? status, UserService users, HttpContext context) =>
{
    if (!IsApiAdmin(context))
        return Results.Forbid();

    return Results.Ok(users.ListRegistrationRequests(GetApiCompanyId(context), status));
});

app.MapPost("/api/admin/registration-requests/{id:int}/approve", (int id, UserService users, AuditLogService audit, HttpContext context) =>
{
    if (!IsApiAdmin(context))
        return Results.Forbid();

    var adminUserId = GetApiUserId(context) ?? 0;
    var result = users.ApproveRegistrationRequest(GetApiCompanyId(context), id, adminUserId);
    audit.Record(GetApiCompanyId(context), adminUserId, GetApiUserName(context), "registration.approve", "registration_request", id.ToString(), success: true);
    return Results.Ok(result);
}).DisableAntiforgery();

app.MapPost("/api/admin/registration-requests/{id:int}/reject", (int id, RejectRegistrationRequest request, UserService users, AuditLogService audit, HttpContext context) =>
{
    if (!IsApiAdmin(context))
        return Results.Forbid();

    var adminUserId = GetApiUserId(context) ?? 0;
    var result = users.RejectRegistrationRequest(GetApiCompanyId(context), id, adminUserId, request.Reason);
    audit.Record(GetApiCompanyId(context), adminUserId, GetApiUserName(context), "registration.reject", "registration_request", id.ToString(), success: true, details: request.Reason);
    return Results.Ok(result);
}).DisableAntiforgery();

app.MapGet("/api/admin/llm-settings", (LlmSettingsService llmSettings, HttpContext context) =>
{
    if (!IsApiAdmin(context))
        return Results.Forbid();

    return Results.Ok(llmSettings.Get(GetApiCompanyId(context)));
});

app.MapPost("/api/admin/llm-settings", (UpdateLlmSettingsRequest request, LlmSettingsService llmSettings, AuditLogService audit, HttpContext context) =>
{
    if (!IsApiAdmin(context))
        return Results.Forbid();

    var settings = llmSettings.Save(request, GetApiCompanyId(context));
    audit.Record(GetApiCompanyId(context), GetApiUserId(context), GetApiUserName(context), "llm_settings.update", "llm_settings", success: true, details: settings.Provider);
    return Results.Ok(settings);
}).DisableAntiforgery();

app.MapPost("/api/query", async (
    QueryRequest request,
    NlSqlEngine engine,
    HttpContext context,
    TenantContext tenantContext,
    AuditLogService audit,
    CancellationToken cancellationToken) =>
{
    if (context.User.Identity?.IsAuthenticated != true)
        return Results.Unauthorized();

    if (string.IsNullOrWhiteSpace(request.Text))
        return Results.BadRequest(new { error = "Query text is empty." });

    var companyId = GetApiCompanyId(context);
    var queryText = request.Text.Trim();
    using var tenantScope = tenantContext.Use(companyId);
    var result = await engine.RunAsync(
        queryText,
        request.History,
        request.PreviousIntent,
        cancellationToken,
        userKey: GetApiUserName(context));
    result.UserQuery = queryText;
    audit.Record(companyId, GetApiUserId(context), GetApiUserName(context), "query.run", "query", success: string.IsNullOrWhiteSpace(result.Error), details: queryText);
    return Results.Ok(result);
}).DisableAntiforgery();

app.MapPost("/api/query/jobs", async (
    QueryJobSubmitRequest request,
    HttpContext context,
    BackgroundQueryService jobs,
    AuditLogService audit,
    CancellationToken cancellationToken) =>
{
    if (context.User.Identity?.IsAuthenticated != true)
        return Results.Unauthorized();

    try
    {
        var job = await jobs.EnqueueAsync(request, GetApiCompanyId(context), GetApiUserId(context), GetApiUserName(context), cancellationToken);
        audit.Record(GetApiCompanyId(context), GetApiUserId(context), GetApiUserName(context), "query.background.enqueue", "query_job", job.Id, success: true, details: request.Text);
        return Results.Accepted($"/api/query/jobs/{job.Id}", job);
    }
    catch (InvalidOperationException exception)
    {
        audit.Record(GetApiCompanyId(context), GetApiUserId(context), GetApiUserName(context), "query.background.enqueue", "query_job", success: false, details: exception.Message);
        return Results.BadRequest(new { error = exception.Message });
    }
}).DisableAntiforgery();

app.MapGet("/api/query/jobs/{id}", (
    string id,
    HttpContext context,
    BackgroundQueryService jobs) =>
{
    if (context.User.Identity?.IsAuthenticated != true)
        return Results.Unauthorized();

    var job = jobs.Get(id, GetApiCompanyId(context), GetApiUserName(context), IsApiAdmin(context));
    return job == null ? Results.NotFound(new { error = "Query job not found." }) : Results.Ok(job);
});

app.MapDelete("/api/query/jobs/{id}", (
    string id,
    HttpContext context,
    BackgroundQueryService jobs,
    AuditLogService audit) =>
{
    if (context.User.Identity?.IsAuthenticated != true)
        return Results.Unauthorized();

    var canceled = jobs.Cancel(id, GetApiCompanyId(context), GetApiUserName(context), IsApiAdmin(context));
    if (!canceled)
        return Results.NotFound(new { error = "Query job not found." });

    audit.Record(GetApiCompanyId(context), GetApiUserId(context), GetApiUserName(context), "query.background.cancel", "query_job", id, success: true);
    return Results.NoContent();
});

app.MapGet("/api/reports", (ReportService reports, HttpContext context) =>
{
    if (context.User.Identity?.IsAuthenticated != true)
        return Results.Unauthorized();

    var userName = GetApiUserName(context);
    return Results.Ok(reports.ListForAuthor(GetApiCompanyId(context), userName));
});

app.MapPost("/api/reports", (SaveReportRequest request, ReportService reports, HttpContext context, AuditLogService audit) =>
{
    if (context.User.Identity?.IsAuthenticated != true)
        return Results.Unauthorized();

    if (string.IsNullOrWhiteSpace(request.Name))
        return Results.BadRequest(new { error = "Report name is empty." });
    if (string.IsNullOrWhiteSpace(request.IntentJson) || string.IsNullOrWhiteSpace(request.Sql))
        return Results.BadRequest(new { error = "Report payload is incomplete." });

    var report = new Report
    {
        Name = request.Name.Trim(),
        CompanyId = GetApiCompanyId(context),
        UserQuery = request.UserQuery?.Trim() ?? string.Empty,
        IntentJson = request.IntentJson,
        Sql = request.Sql,
        Visualization = string.IsNullOrWhiteSpace(request.Visualization) ? "table" : request.Visualization.Trim(),
        Author = GetApiUserName(context),
        CreatedAt = DateTime.UtcNow
    };

    report.Id = reports.Save(report);
    audit.Record(report.CompanyId, GetApiUserId(context), report.Author, "report.save", "report", report.Id.ToString(), success: true);
    return Results.Ok(report);
}).DisableAntiforgery();

app.MapPost("/api/reports/{id:int}/rerun", (
    int id,
    NlSqlEngine engine,
    ReportService reports,
    HttpContext context,
    TenantContext tenantContext,
    AuditLogService audit) =>
{
    if (context.User.Identity?.IsAuthenticated != true)
        return Results.Unauthorized();

    var companyId = GetApiCompanyId(context);
    var report = reports.GetForAuthor(companyId, id, GetApiUserName(context), IsApiAdmin(context));
    if (report == null)
        return Results.NotFound(new { error = "Report not found." });

    using var tenantScope = tenantContext.Use(companyId);
    var result = engine.ReplayFromReport(report.IntentJson, userKey: GetApiUserName(context));
    result.UserQuery = report.UserQuery;
    audit.Record(companyId, GetApiUserId(context), GetApiUserName(context), "report.rerun", "report", id.ToString(), success: true);
    return Results.Ok(result);
}).DisableAntiforgery();

app.MapDelete("/api/reports/{id:int}", (int id, ReportService reports, HttpContext context, AuditLogService audit) =>
{
    if (context.User.Identity?.IsAuthenticated != true)
        return Results.Unauthorized();

    var companyId = GetApiCompanyId(context);
    reports.DeleteForAuthor(companyId, id, GetApiUserName(context), IsApiAdmin(context));
    audit.Record(companyId, GetApiUserId(context), GetApiUserName(context), "report.delete", "report", id.ToString(), success: true);
    return Results.NoContent();
});

app.MapStaticAssets();
app.MapRazorComponents<App>()
    .AddInteractiveServerRenderMode();

app.Run();

static string NormalizeReturnUrl(string? returnUrl)
{
    if (string.IsNullOrWhiteSpace(returnUrl))
    {
        return "/";
    }

    return returnUrl.StartsWith("/", StringComparison.Ordinal) && !returnUrl.StartsWith("//", StringComparison.Ordinal)
        ? returnUrl
        : "/";
}

static string GetApiUserName(HttpContext context) =>
    context.User.Identity?.IsAuthenticated == true
        ? context.User.Identity.Name ?? "desktop"
        : "desktop";

static int GetApiCompanyId(HttpContext context) =>
    context.User.Identity?.IsAuthenticated == true
        ? context.User.GetCompanyId()
        : CompanyDefaults.DefaultCompanyId;

static int? GetApiUserId(HttpContext context) =>
    int.TryParse(context.User.FindFirstValue(ClaimTypes.NameIdentifier), out var id) ? id : null;

static bool IsApiAdmin(HttpContext context) =>
    context.User.Identity?.IsAuthenticated == true && context.User.IsAdmin();

static string BuildRetryAfterMessage(TimeSpan retryAfter) =>
    $"Too many login attempts. Try again in {Math.Max(1, (int)Math.Ceiling(retryAfter.TotalSeconds))} sec.";
