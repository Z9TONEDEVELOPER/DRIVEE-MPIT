using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using DriveeDataSpace.Core.Models;
using Microsoft.Extensions.Configuration;

namespace DriveeDataSpace.Core.Services;

public sealed class AuthTokenService
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);
    private readonly byte[] _signingKey;
    private readonly TimeSpan _lifetime;

    public AuthTokenService(IConfiguration configuration)
    {
        var configuredKey = configuration["Auth:ApiTokenSigningKey"];
        if (string.IsNullOrWhiteSpace(configuredKey))
            configuredKey = "drivee-bi-local-dev-token-signing-key-change-me";

        _signingKey = Encoding.UTF8.GetBytes(configuredKey);
        _lifetime = TimeSpan.FromHours(
            int.TryParse(configuration["Auth:ApiTokenLifetimeHours"], out var hours)
                ? Math.Clamp(hours, 1, 24 * 30)
                : 12);
    }

    public AuthSession CreateSession(AppUser user)
    {
        var expiresAt = DateTimeOffset.UtcNow.Add(_lifetime);
        var session = new AuthUserSession(
            user.Id,
            user.Username,
            user.DisplayName,
            user.Role,
            expiresAt);

        var payload = Base64UrlEncode(JsonSerializer.SerializeToUtf8Bytes(session, JsonOptions));
        var signature = Base64UrlEncode(Sign(payload));
        return new AuthSession($"{payload}.{signature}", expiresAt, user);
    }

    public AuthUserSession? Validate(string? token)
    {
        if (string.IsNullOrWhiteSpace(token))
            return null;

        var parts = token.Split('.', 2);
        if (parts.Length != 2)
            return null;

        var expectedSignature = Base64UrlEncode(Sign(parts[0]));
        if (!CryptographicOperations.FixedTimeEquals(
                Encoding.ASCII.GetBytes(expectedSignature),
                Encoding.ASCII.GetBytes(parts[1])))
        {
            return null;
        }

        try
        {
            var payload = Base64UrlDecode(parts[0]);
            var session = JsonSerializer.Deserialize<AuthUserSession>(payload, JsonOptions);
            if (session == null || session.ExpiresAt <= DateTimeOffset.UtcNow)
                return null;

            return session;
        }
        catch (JsonException)
        {
            return null;
        }
        catch (FormatException)
        {
            return null;
        }
    }

    private byte[] Sign(string payload)
    {
        using var hmac = new HMACSHA256(_signingKey);
        return hmac.ComputeHash(Encoding.ASCII.GetBytes(payload));
    }

    private static string Base64UrlEncode(byte[] value) =>
        Convert.ToBase64String(value)
            .TrimEnd('=')
            .Replace('+', '-')
            .Replace('/', '_');

    private static byte[] Base64UrlDecode(string value)
    {
        var padded = value.Replace('-', '+').Replace('_', '/');
        padded = padded.PadRight(padded.Length + (4 - padded.Length % 4) % 4, '=');
        return Convert.FromBase64String(padded);
    }
}
