using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Coflnet.Sky.Core;
using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using NUnit.Framework;

namespace Coflnet.Sky.Updater.Tests;

public class CancelledAuctionDetectorTests
{
    private class FakeHandler : HttpMessageHandler
    {
        public int Calls;
        public HttpStatusCode Status = HttpStatusCode.OK;
        public string Body = "[]";
        public bool Hang;
        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            Calls++;
            if (Hang)
                await Task.Delay(TimeSpan.FromSeconds(30), cancellationToken);
            return new HttpResponseMessage(Status) { Content = new StringContent(Body) };
        }
    }

    private FakeHandler handler;
    private Mock<IProducer<string, SaveAuction>> producer;
    private List<(string topic, SaveAuction auction)> produced;
    private CancelledAuctionDetector detector;

    [SetUp]
    public void Setup()
    {
        handler = new FakeHandler();
        produced = new();
        producer = new Mock<IProducer<string, SaveAuction>>();
        producer.Setup(p => p.Produce(It.IsAny<string>(), It.IsAny<Message<string, SaveAuction>>(), It.IsAny<Action<DeliveryReport<string, SaveAuction>>>()))
            .Callback<string, Message<string, SaveAuction>, Action<DeliveryReport<string, SaveAuction>>>((t, m, _) => produced.Add((t, m.Value)));
        var config = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string>
        {
            ["INDEXER_BASE_URL"] = "http://indexer",
            ["TOPICS:MISSING_AUCTION"] = "sky-canceledauction"
        }).Build();
        detector = new CancelledAuctionDetector(new HttpClient(handler), config, NullLogger<CancelledAuctionDetector>.Instance);
    }

    private static SaveAuction Hyp(long uid) => new() { UId = uid, Uuid = Guid.NewGuid().ToString("N") };

    [Test]
    public async Task ProducesExactlyOursMinusHypixel()
    {
        handler.Body = "[1,2,3]";
        var count = await detector.DetectAndProduceAsync("seller1", new[] { Hyp(2), Hyp(9) }, producer.Object, "src", CancellationToken.None);
        Assert.That(count, Is.EqualTo(2));
        Assert.That(produced.Select(p => p.auction.UId).OrderBy(u => u), Is.EqualTo(new long[] { 1, 3 }));
        Assert.That(produced.All(p => p.topic == "sky-canceledauction"));
    }

    [Test]
    public async Task StubCarriesIdentityAndContext()
    {
        handler.Body = "[5]";
        var before = DateTime.UtcNow;
        await detector.DetectAndProduceAsync("seller1", Array.Empty<SaveAuction>(), producer.Object, "pre-api", CancellationToken.None);
        var stub = produced.Single().auction;
        Assert.That(stub.UId, Is.EqualTo(5));
        Assert.That(stub.AuctioneerId, Is.EqualTo("seller1"));
        Assert.That(stub.End, Is.GreaterThanOrEqualTo(before));
        Assert.That(stub.Context["cancelled"], Is.EqualTo("pre-api"));
    }

    [Test]
    public async Task NothingWhenIndexerUnavailable()
    {
        handler.Status = HttpStatusCode.ServiceUnavailable;
        handler.Body = "[1]";
        var count = await detector.DetectAndProduceAsync("seller1", Array.Empty<SaveAuction>(), producer.Object, "src", CancellationToken.None);
        Assert.That(count, Is.EqualTo(0));
        Assert.That(produced, Is.Empty);
    }

    [Test]
    public async Task NothingOnTimeout()
    {
        handler.Hang = true;
        var count = await detector.DetectAndProduceAsync("seller1", Array.Empty<SaveAuction>(), producer.Object, "src", CancellationToken.None);
        Assert.That(count, Is.EqualTo(0));
        Assert.That(produced, Is.Empty);
    }

    [Test]
    public async Task NothingWhenOursEmpty()
    {
        handler.Body = "[]";
        var count = await detector.DetectAndProduceAsync("seller1", new[] { Hyp(1) }, producer.Object, "src", CancellationToken.None);
        Assert.That(count, Is.EqualTo(0));
        Assert.That(produced, Is.Empty);
    }

    [Test]
    public async Task CachesIndexerResponseFor30Seconds()
    {
        handler.Body = "[1]";
        await detector.DetectAndProduceAsync("seller1", new[] { Hyp(1) }, producer.Object, "src", CancellationToken.None);
        await detector.DetectAndProduceAsync("seller1", new[] { Hyp(1) }, producer.Object, "src", CancellationToken.None);
        Assert.That(handler.Calls, Is.EqualTo(1));
        await detector.DetectAndProduceAsync("seller2", new[] { Hyp(1) }, producer.Object, "src", CancellationToken.None);
        Assert.That(handler.Calls, Is.EqualTo(2));
    }
}
