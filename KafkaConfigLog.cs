using System;
using System.Collections.Generic;
using System.Linq;

namespace Coflnet.Sky.Updater;

/// <summary>
/// Turns a Kafka client config into a log-safe view: secret values (passwords, keys, tokens) are replaced.
/// </summary>
public static class KafkaConfigLog
{
    public const string Redacted = "<redacted>";
    private static readonly string[] SecretKeyParts = { "password", "secret", "token", "key.pem", "certificate.pem", "jaas" };

    public static List<KeyValuePair<string, string>> Redact(IEnumerable<KeyValuePair<string, string>> config)
    {
        return config.Select(RedactEntry).ToList();
    }

    private static KeyValuePair<string, string> RedactEntry(KeyValuePair<string, string> entry)
    {
        if (!IsSecretKey(entry.Key))
            return entry;
        return new KeyValuePair<string, string>(entry.Key, Redacted);
    }

    private static bool IsSecretKey(string key)
    {
        return SecretKeyParts.Any(part => key.Contains(part, StringComparison.OrdinalIgnoreCase));
    }
}
