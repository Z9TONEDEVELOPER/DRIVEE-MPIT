using System.Security.Claims;
using DriveeDataSpace.Core.Models;

namespace DriveeDataSpace.Core.Services;

public static class UserPrincipalExtensions
{
    public static string GetUsername(this ClaimsPrincipal user) =>
        user.Identity?.Name ?? "";

    public static string GetDisplayName(this ClaimsPrincipal user) =>
        user.FindFirst(ClaimTypes.GivenName)?.Value
        ?? user.Identity?.Name
        ?? "User";

    public static bool IsAdmin(this ClaimsPrincipal user) =>
        user.IsInRole(AppRoles.Admin);

    public static int? GetUserId(this ClaimsPrincipal user)
    {
        var value = user.FindFirst(ClaimTypes.NameIdentifier)?.Value;
        return int.TryParse(value, out var id) ? id : null;
    }

    public static int GetCompanyId(this ClaimsPrincipal user)
    {
        var value = user.FindFirst("company_id")?.Value;
        return int.TryParse(value, out var id) && id > 0 ? id : CompanyDefaults.DefaultCompanyId;
    }
}
