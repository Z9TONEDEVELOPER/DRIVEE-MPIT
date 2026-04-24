using System.Security.Claims;
using DriveeDataSpace.Web.Models;

namespace DriveeDataSpace.Web.Services;

public static class UserPrincipalExtensions
{
    public static string GetUsername(this ClaimsPrincipal user) =>
        user.Identity?.Name ?? "";

    public static string GetDisplayName(this ClaimsPrincipal user) =>
        user.FindFirstValue(ClaimTypes.GivenName)
        ?? user.Identity?.Name
        ?? "User";

    public static bool IsAdmin(this ClaimsPrincipal user) =>
        user.IsInRole(AppRoles.Admin);

    public static int? GetUserId(this ClaimsPrincipal user)
    {
        var value = user.FindFirstValue(ClaimTypes.NameIdentifier);
        return int.TryParse(value, out var id) ? id : null;
    }
}
