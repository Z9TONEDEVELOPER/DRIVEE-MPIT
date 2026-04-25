using System.Security.Cryptography;
using System.Text;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;

namespace DriveeDataSpace.Core.Services;

public sealed class SecretProtector
{
    private const string Prefix = "enc:v1:";
    private readonly byte[] _key;
    private readonly IReadOnlyList<byte[]> _previousKeys;

    public SecretProtector(IConfiguration configuration, IHostEnvironment environment)
    {
        const string devDefaultKey = "drivee-bi-local-dev-secret-protection-key-change-me";
        var configured = configuration["Security:SecretProtectionKey"]
            ?? configuration["Auth:ApiTokenSigningKey"]
            ?? devDefaultKey;
        const string authDevDefaultKey = "drivee-bi-local-dev-token-signing-key-change-me";
        if (environment.IsProduction() &&
            (string.Equals(configured, devDefaultKey, StringComparison.Ordinal) ||
             string.Equals(configured, authDevDefaultKey, StringComparison.Ordinal)))
        {
            throw new InvalidOperationException("Security:SecretProtectionKey must be configured from secret storage in production.");
        }
        if (configured.Length < 32)
            throw new InvalidOperationException("Security:SecretProtectionKey must contain at least 32 characters.");

        _key = SHA256.HashData(Encoding.UTF8.GetBytes(configured));
        _previousKeys = (configuration.GetSection("Security:PreviousSecretProtectionKeys").Get<string[]>()?
            .Where(key => !string.IsNullOrWhiteSpace(key) && key.Length >= 32)
            .Select(key => SHA256.HashData(Encoding.UTF8.GetBytes(key)))
            .ToList())
            ?? new List<byte[]>();
    }

    public string Protect(string value)
    {
        if (string.IsNullOrEmpty(value))
            return "";

        var nonce = RandomNumberGenerator.GetBytes(12);
        var plaintext = Encoding.UTF8.GetBytes(value);
        var ciphertext = new byte[plaintext.Length];
        var tag = new byte[16];

        using var aes = new AesGcm(_key, tag.Length);
        aes.Encrypt(nonce, plaintext, ciphertext, tag);

        return Prefix +
            Convert.ToBase64String(nonce) + "." +
            Convert.ToBase64String(tag) + "." +
            Convert.ToBase64String(ciphertext);
    }

    public string Unprotect(string value)
    {
        if (string.IsNullOrEmpty(value))
            return "";

        if (!value.StartsWith(Prefix, StringComparison.Ordinal))
            return value;

        var parts = value[Prefix.Length..].Split('.', 3);
        if (parts.Length != 3)
            throw new InvalidOperationException("Protected secret format is invalid.");

        var nonce = Convert.FromBase64String(parts[0]);
        var tag = Convert.FromBase64String(parts[1]);
        var ciphertext = Convert.FromBase64String(parts[2]);
        var plaintext = new byte[ciphertext.Length];

        if (TryDecrypt(_key, nonce, tag, ciphertext, plaintext))
            return Encoding.UTF8.GetString(plaintext);

        foreach (var previousKey in _previousKeys)
        {
            if (TryDecrypt(previousKey, nonce, tag, ciphertext, plaintext))
                return Encoding.UTF8.GetString(plaintext);
        }

        throw new InvalidOperationException("Protected secret cannot be decrypted with configured keys.");
    }

    private static bool TryDecrypt(byte[] key, byte[] nonce, byte[] tag, byte[] ciphertext, byte[] plaintext)
    {
        try
        {
            Array.Clear(plaintext);
            using var aes = new AesGcm(key, tag.Length);
            aes.Decrypt(nonce, ciphertext, tag, plaintext);
            return true;
        }
        catch (CryptographicException)
        {
            return false;
        }
    }

    public static string MaskConnectionString(string connectionString)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
            return "";

        if (!connectionString.Contains('=', StringComparison.Ordinal))
            return MaskPath(connectionString);

        var parts = connectionString.Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        for (var i = 0; i < parts.Length; i++)
        {
            var separator = parts[i].IndexOf('=');
            if (separator <= 0)
                continue;

            var key = parts[i][..separator].Trim();
            if (IsSensitiveKey(key))
                parts[i] = $"{key}=***";
        }

        return string.Join("; ", parts);
    }

    public static bool IsMasked(string value) =>
        value.Contains("***", StringComparison.Ordinal);

    private static bool IsSensitiveKey(string key) =>
        key.Contains("password", StringComparison.OrdinalIgnoreCase) ||
        key.Equals("pwd", StringComparison.OrdinalIgnoreCase) ||
        key.Contains("user", StringComparison.OrdinalIgnoreCase) ||
        key.Contains("uid", StringComparison.OrdinalIgnoreCase) ||
        key.Contains("token", StringComparison.OrdinalIgnoreCase) ||
        key.Contains("key", StringComparison.OrdinalIgnoreCase);

    private static string MaskPath(string path)
    {
        var fileName = Path.GetFileName(path);
        return string.IsNullOrWhiteSpace(fileName) ? "***" : $"***{Path.DirectorySeparatorChar}{fileName}";
    }
}
