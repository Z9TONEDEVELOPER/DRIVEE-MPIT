using NexusDataSpace.Core.Models;
using Microsoft.Extensions.Configuration;

namespace NexusDataSpace.Core.Services;

public sealed class RetentionService
{
    private readonly IConfiguration _configuration;
    private readonly AuditLogService _auditLog;
    private readonly ReportService _reports;

    public RetentionService(
        IConfiguration configuration,
        AuditLogService auditLog,
        ReportService reports)
    {
        _configuration = configuration;
        _auditLog = auditLog;
        _reports = reports;
    }

    public RetentionCleanupResult Apply(int companyId)
    {
        var auditDays = ReadBoundedInt(_configuration, "Retention:AuditLogDays", 90, 1, 3_650);
        var reportDays = ReadBoundedInt(_configuration, "Retention:ReportsDays", 365, 1, 3_650);
        var deletedAudit = _auditLog.ApplyRetention(companyId, auditDays);
        var deletedReports = _reports.ApplyRetention(companyId, reportDays);
        return new RetentionCleanupResult(deletedAudit, deletedReports);
    }

    private static int ReadBoundedInt(IConfiguration configuration, string key, int fallback, int min, int max) =>
        int.TryParse(configuration[key], out var value) ? Math.Clamp(value, min, max) : fallback;
}
