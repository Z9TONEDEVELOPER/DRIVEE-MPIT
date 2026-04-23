using Microsoft.AspNetCore.Components;
using DriveeDataSpace.Web.Components;
using DriveeDataSpace.Web.Services;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddRazorComponents()
    .AddInteractiveServerComponents();

builder.Services.AddHttpClient("llm");
builder.Services.AddSingleton<SemanticLayer>();
builder.Services.AddSingleton<DatasetSeeder>();
builder.Services.AddSingleton<SqlBuilder>();
builder.Services.AddSingleton<QueryExecutor>();
builder.Services.AddSingleton<LlmService>();
builder.Services.AddScoped<NlSqlEngine>();
builder.Services.AddSingleton<ReportService>();

var app = builder.Build();

app.Services.GetRequiredService<DatasetSeeder>().EnsureSeeded();
_ = app.Services.GetRequiredService<ReportService>();

if (!app.Environment.IsDevelopment())
{
    app.UseExceptionHandler("/Error", createScopeForErrors: true);
    app.UseHsts();
}

app.UseHttpsRedirection();
app.UseAntiforgery();

app.MapStaticAssets();
app.MapRazorComponents<App>()
    .AddInteractiveServerRenderMode();

app.Run();
