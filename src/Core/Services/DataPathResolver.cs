using Microsoft.Extensions.Hosting;

namespace NexusDataSpace.Core.Services;

internal static class DataPathResolver
{
    public static string Resolve(IHostEnvironment environment, string? configuredPath, string fallbackPath)
    {
        var path = string.IsNullOrWhiteSpace(configuredPath) ? fallbackPath : configuredPath;
        return Path.IsPathRooted(path)
            ? path
            : Path.Combine(environment.ContentRootPath, path);
    }
}
