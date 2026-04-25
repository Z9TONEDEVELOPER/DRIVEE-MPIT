using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;

namespace DriveeDataSpace.Core.Services;

public sealed class ProductionMetadataGuardService
{
    private readonly IConfiguration _configuration;
    private readonly IHostEnvironment _environment;

    public ProductionMetadataGuardService(IConfiguration configuration, IHostEnvironment environment)
    {
        _configuration = configuration;
        _environment = environment;
    }

    public void EnsureProductionReady()
    {
        if (!_environment.IsProduction())
            return;

        var provider = _configuration["Data:MetadataProvider"] ?? "sqlite";
        if (!string.Equals(provider, "postgresql", StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException(
                "Production metadata DB must be PostgreSQL. Set Data:MetadataProvider=postgresql and provide the production metadata connection before running in production.");
        }
    }
}
