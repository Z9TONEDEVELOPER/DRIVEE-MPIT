using System.Security.Claims;
using DriveeDataSpace.Web.Components;
using DriveeDataSpace.Core.Models;
using DriveeDataSpace.Core.Services;
using DriveeDataSpace.Web.Services;
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
        options.Cookie.Name = builder.Configuration["Auth:CookieName"] ?? "drivee.bi.auth";
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
