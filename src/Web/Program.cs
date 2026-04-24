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
        options.LoginPath = "/login";
        options.AccessDeniedPath = "/login";
        options.SlidingExpiration = true;
        options.ExpireTimeSpan = TimeSpan.FromHours(12);
    });
builder.Services.AddAuthorization();
builder.Services.AddHttpContextAccessor();

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
builder.Services.AddSingleton<LlmService>();
builder.Services.AddSingleton<UserService>();
builder.Services.AddSingleton<EmailService>();
builder.Services.AddScoped<NlSqlEngine>();
builder.Services.AddScoped<WorkspaceSessionState>();
builder.Services.AddSingleton<ReportService>();

var app = builder.Build();

_ = app.Services.GetRequiredService<DataSourceService>();
app.Services.GetRequiredService<DatasetSeeder>().EnsureSeeded();
_ = app.Services.GetRequiredService<ReportService>();
_ = app.Services.GetRequiredService<UserService>();

if (!app.Environment.IsDevelopment())
{
    app.UseExceptionHandler("/Error", createScopeForErrors: true);
    app.UseHsts();
}

app.UseHttpsRedirection();
app.UseAuthentication();
app.UseAuthorization();
app.UseAntiforgery();

app.MapPost("/auth/login", async (HttpContext context, UserService users) =>
{
    var form = await context.Request.ReadFormAsync();
    var username = form["username"].ToString();
    var password = form["password"].ToString();
    var returnUrl = NormalizeReturnUrl(form["returnUrl"].ToString());

    var user = users.Authenticate(username, password);
    if (user == null)
    {
        var loginUrl = $"/login?error={Uri.EscapeDataString("Неверный логин или пароль.")}&returnUrl={Uri.EscapeDataString(returnUrl)}";
        context.Response.Redirect(loginUrl);
        return;
    }

    var claims = new List<Claim>
    {
        new(ClaimTypes.NameIdentifier, user.Id.ToString()),
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
    CancellationToken cancellationToken) =>
{
    if (string.IsNullOrWhiteSpace(request.Text))
        return Results.BadRequest(new { error = "Query text is empty." });

    var result = await engine.RunAsync(
        request.Text.Trim(),
        request.History,
        request.PreviousIntent,
        cancellationToken);
    result.UserQuery = request.Text.Trim();
    return Results.Ok(result);
}).DisableAntiforgery().AllowAnonymous();

app.MapGet("/api/reports", (ReportService reports, HttpContext context) =>
{
    var userName = GetApiUserName(context);
    return Results.Ok(reports.ListForAuthor(userName));
}).AllowAnonymous();

app.MapPost("/api/reports", (SaveReportRequest request, ReportService reports, HttpContext context) =>
{
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
        Author = GetApiUserName(context),
        CreatedAt = DateTime.UtcNow
    };

    report.Id = reports.Save(report);
    return Results.Ok(report);
}).DisableAntiforgery().AllowAnonymous();

app.MapPost("/api/reports/{id:int}/rerun", (
    int id,
    NlSqlEngine engine,
    ReportService reports,
    HttpContext context) =>
{
    var report = reports.GetForAuthor(id, GetApiUserName(context), IsApiAdmin(context));
    if (report == null)
        return Results.NotFound(new { error = "Report not found." });

    var result = engine.ReplayFromReport(report.IntentJson);
    result.UserQuery = report.UserQuery;
    return Results.Ok(result);
}).DisableAntiforgery().AllowAnonymous();

app.MapDelete("/api/reports/{id:int}", (int id, ReportService reports, HttpContext context) =>
{
    reports.DeleteForAuthor(id, GetApiUserName(context), IsApiAdmin(context));
    return Results.NoContent();
}).AllowAnonymous();

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

static bool IsApiAdmin(HttpContext context) =>
    context.User.Identity?.IsAuthenticated == true && context.User.IsInRole(AppRoles.Admin);
