namespace DriveeDataSpace.Core.Models;

public static class AppRoles
{
    public const string Owner = "Owner";
    public const string Admin = "Admin";
    public const string Analyst = "Analyst";
    public const string Viewer = "Viewer";
    public const string User = Analyst;
    public const string AdminAccess = Owner + "," + Admin;

    public static IReadOnlyList<string> All { get; } = new[] { Owner, Admin, Analyst, Viewer };

    public static string Normalize(string? role)
    {
        if (string.Equals(role, Owner, StringComparison.OrdinalIgnoreCase))
            return Owner;
        if (string.Equals(role, Admin, StringComparison.OrdinalIgnoreCase))
            return Admin;
        if (string.Equals(role, Viewer, StringComparison.OrdinalIgnoreCase))
            return Viewer;
        return Analyst;
    }

    public static bool CanAdminister(string? role) =>
        string.Equals(role, Owner, StringComparison.OrdinalIgnoreCase) ||
        string.Equals(role, Admin, StringComparison.OrdinalIgnoreCase);
}

public class AppUser
{
    public int Id { get; set; }
    public int CompanyId { get; set; } = CompanyDefaults.DefaultCompanyId;
    public string CompanyName { get; set; } = CompanyDefaults.DefaultCompanyName;
    public string Username { get; set; } = "";
    public string? Email { get; set; }
    public string DisplayName { get; set; } = "";
    public string Role { get; set; } = AppRoles.User;
    public bool IsActive { get; set; } = true;
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
    public DateTime? LastLoginAt { get; set; }
}

public class AppUserSummary : AppUser
{
    public int ReportCount { get; set; }
}

public sealed class AppUserDetail : AppUserSummary
{
    public List<Report> RecentReports { get; set; } = new();
}

public sealed class SeedUserOptions
{
    public string Username { get; set; } = "";
    public string Email { get; set; } = "";
    public string DisplayName { get; set; } = "";
    public string Password { get; set; } = "";
    public string Role { get; set; } = AppRoles.User;
    public string Company { get; set; } = CompanyDefaults.DefaultCompanyName;
}

public static class CompanyDefaults
{
    public const int DefaultCompanyId = 1;
    public const string DefaultCompanyName = "Default company";
}

public sealed class Company
{
    public int Id { get; set; } = CompanyDefaults.DefaultCompanyId;
    public string Name { get; set; } = CompanyDefaults.DefaultCompanyName;
    public string Slug { get; set; } = "default";
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
    public int UserCount { get; set; }
    public int ActiveUserCount { get; set; }
    public int DataSourceCount { get; set; }
    public int VerifiedDataSourceCount { get; set; }
}

public sealed record UpdateCompanyRequest(string Name);

public static class RegistrationRequestStatuses
{
    public const string Pending = "Pending";
    public const string Approved = "Approved";
    public const string Rejected = "Rejected";
}

public sealed class RegistrationRequest
{
    public int Id { get; set; }
    public int CompanyId { get; set; } = CompanyDefaults.DefaultCompanyId;
    public string CompanyName { get; set; } = CompanyDefaults.DefaultCompanyName;
    public string Email { get; set; } = "";
    public string DisplayName { get; set; } = "";
    public string Organization { get; set; } = "";
    public string Comment { get; set; } = "";
    public string Status { get; set; } = RegistrationRequestStatuses.Pending;
    public string? RejectionReason { get; set; }
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
    public DateTime? ReviewedAt { get; set; }
    public int? ReviewedByUserId { get; set; }
    public string? ReviewedByDisplayName { get; set; }
}

public sealed record RegistrationRequestInput(
    string DisplayName,
    string Email,
    string Password,
    string Organization,
    string Comment);

public sealed record RegistrationDecisionResult(
    RegistrationRequest Request,
    AppUser? CreatedUser);

public sealed class LocalEmailMessage
{
    public int Id { get; set; }
    public string To { get; set; } = "";
    public string Subject { get; set; } = "";
    public string Body { get; set; } = "";
    public string Category { get; set; } = "";
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
}
