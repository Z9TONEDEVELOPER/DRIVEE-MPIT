var builder = WebApplication.CreateBuilder(args);

builder.Services.AddOpenApi();
builder.Services.AddHttpClient("drivee-backend", client =>
{
    client.BaseAddress = new Uri(builder.Configuration["Backend:BaseUrl"] ?? "http://localhost:5012");
    client.Timeout = TimeSpan.FromSeconds(
        int.TryParse(builder.Configuration["Backend:TimeoutSeconds"], out var timeoutSeconds)
            ? timeoutSeconds
            : 120);
});

var app = builder.Build();

if (app.Environment.IsDevelopment())
    app.MapOpenApi();

app.UseHttpsRedirection();

app.MapGet("/health", () => Results.Ok(new
{
    status = "ok",
    service = "DriveeDataSpace.Api"
}));

app.MapPost("/api/query", (HttpContext context, IHttpClientFactory httpClientFactory) =>
    ForwardAsync(context, httpClientFactory, HttpMethod.Post, "/api/query"))
    .DisableAntiforgery();

app.MapGet("/api/reports", (HttpContext context, IHttpClientFactory httpClientFactory) =>
    ForwardAsync(context, httpClientFactory, HttpMethod.Get, "/api/reports"));

app.MapPost("/api/reports", (HttpContext context, IHttpClientFactory httpClientFactory) =>
    ForwardAsync(context, httpClientFactory, HttpMethod.Post, "/api/reports"))
    .DisableAntiforgery();

app.MapPost("/api/reports/{id:int}/rerun", (int id, HttpContext context, IHttpClientFactory httpClientFactory) =>
    ForwardAsync(context, httpClientFactory, HttpMethod.Post, $"/api/reports/{id}/rerun"))
    .DisableAntiforgery();

app.MapDelete("/api/reports/{id:int}", (int id, HttpContext context, IHttpClientFactory httpClientFactory) =>
    ForwardAsync(context, httpClientFactory, HttpMethod.Delete, $"/api/reports/{id}"));

app.Run();

static async Task ForwardAsync(
    HttpContext context,
    IHttpClientFactory httpClientFactory,
    HttpMethod method,
    string path)
{
    var client = httpClientFactory.CreateClient("drivee-backend");
    using var request = new HttpRequestMessage(method, path);

    if (method != HttpMethod.Get && method != HttpMethod.Delete)
    {
        request.Content = new StreamContent(context.Request.Body);
        if (!string.IsNullOrWhiteSpace(context.Request.ContentType))
            request.Content.Headers.TryAddWithoutValidation("Content-Type", context.Request.ContentType);
    }

    using var response = await client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, context.RequestAborted);
    context.Response.StatusCode = (int)response.StatusCode;

    foreach (var header in response.Headers)
        context.Response.Headers[header.Key] = header.Value.ToArray();

    foreach (var header in response.Content.Headers)
        context.Response.Headers[header.Key] = header.Value.ToArray();

    context.Response.Headers.Remove("transfer-encoding");
    await response.Content.CopyToAsync(context.Response.Body, context.RequestAborted);
}
