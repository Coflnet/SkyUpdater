using System.Collections.Generic;
using System.Linq;
using Confluent.Kafka;
using Newtonsoft.Json;
using NUnit.Framework;

namespace Coflnet.Sky.Updater;

public class KafkaConfigLogTests
{
    private const string Password = "s3cretPassw0rd";

    [Test]
    public void SaslPasswordIsNeverInTheOutput()
    {
        var config = new ClientConfig { BootstrapServers = "broker:9092", SaslUsername = "updater", SaslPassword = Password };

        var json = JsonConvert.SerializeObject(KafkaConfigLog.Redact(config));

        Assert.That(json, Does.Not.Contain(Password[..3]));
        Assert.That(json, Does.Contain(KafkaConfigLog.Redacted));
    }

    [Test]
    public void KeyPasswordAndInlineKeyAreRedacted()
    {
        var config = new ClientConfig { SslKeyPassword = Password, SslKeyPem = "PEMDATA", SslKeyLocation = "/pki/client/tls.key" };

        var redacted = KafkaConfigLog.Redact(config).ToDictionary(e => e.Key, e => e.Value);

        Assert.That(redacted["ssl.key.password"], Is.EqualTo(KafkaConfigLog.Redacted));
        Assert.That(redacted["ssl.key.pem"], Is.EqualTo(KafkaConfigLog.Redacted));
        Assert.That(redacted["ssl.key.location"], Is.EqualTo("/pki/client/tls.key"));
    }

    [Test]
    public void NonSecretEntriesAreKept()
    {
        var config = new List<KeyValuePair<string, string>>
        {
            new("bootstrap.servers", "broker:9092"),
            new("sasl.username", "updater"),
            new("security.protocol", "SaslSsl"),
        };

        var redacted = KafkaConfigLog.Redact(config);

        Assert.That(redacted, Is.EqualTo(config));
    }
}
