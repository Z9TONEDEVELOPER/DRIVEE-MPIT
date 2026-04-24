namespace DriveeDataSpace.Web.Models;

public static class AppRoles
{
    public const string Admin = "Admin";
    public const string User = "User";
}

public class AppUser
{
    public int Id { get; set; }
    public string Username { get; set; } = "";
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
    public string DisplayName { get; set; } = "";
    public string Password { get; set; } = "";
    public string Role { get; set; } = AppRoles.User;
}
