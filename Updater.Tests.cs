using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Coflnet.Sky.Core;
using Coflnet.Sky.Updater.Models;
using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using NUnit.Framework;

namespace Coflnet.Sky.Updater.Tests
{
    public class UpdaterTests
    {
        [Test]
        public void TimeFormat()
        {
            Assert.That("Thu, 20 Jan 2022 20:00:00 GMT", Is.EqualTo(Updater.FormatTime(new System.DateTime(2022, 1, 20, 20, 0, 0, System.DateTimeKind.Utc))));
        }

        [TestCase(1)]
        [TestCase(2)]
        [TestCase(3)]
        [TestCase(4)]
        public async Task DelayedPagePublishesNewAuctionsAfterFirstPageAdvances(int pageId)
        {
            var updater = new PageTestUpdater();
            var producer = new FakeProducer();
            var previousIndex = Updater.updaterIndex;
            Updater.updaterIndex = 0;
            var cachedAt = DateTimeOffset.UtcNow.AddMinutes(-10);
            try
            {
                await updater.SavePage(new AuctionPage { Page = pageId, _lastUpdated = cachedAt.ToUnixTimeMilliseconds(), Auctions = new() }, cachedAt.LocalDateTime.AddMinutes(-1), producer);
                // Page 1 advanced five minutes while this page still contained the old snapshot.
                var delayed = new AuctionPage
                {
                    Page = pageId,
                    _lastUpdated = cachedAt.AddMinutes(5).ToUnixTimeMilliseconds(),
                    Auctions = new()
                    {
                        CreateAuction(cachedAt.AddSeconds(30)),
                        CreateAuction(cachedAt.AddMinutes(-2)),
                        CreateAuction(cachedAt.AddMinutes(4))
                    }
                };
                await updater.SavePage(delayed, cachedAt.LocalDateTime.AddMinutes(4), producer);
                Assert.That(producer.Messages.Where(m => m.Topic == Updater.NewAuctionsTopic).Select(m => m.Auction.Uuid),
                    Is.EquivalentTo(new[] { delayed.Auctions[0].Uuid, delayed.Auctions[2].Uuid }));

                producer.Messages.Clear();
                delayed.Auctions = new() { delayed.Auctions[0] };
                await updater.SavePage(delayed, cachedAt.LocalDateTime.AddMinutes(4), producer);
                Assert.That(producer.Messages.Where(m => m.Topic == Updater.NewAuctionsTopic), Is.Empty);

                // A stale first page can also push older listings onto a page with a newer cache.
                delayed.Auctions = new() { CreateAuction(cachedAt.AddMinutes(2)) };
                await updater.SavePage(delayed, cachedAt.LocalDateTime.AddMinutes(-1), producer);
                Assert.That(producer.Messages.Where(m => m.Topic == Updater.NewAuctionsTopic).Select(m => m.Auction.Uuid),
                    Is.EqualTo(new[] { delayed.Auctions[0].Uuid }));
            }
            finally
            {
                Updater.updaterIndex = previousIndex;
            }
        }

        private static Auction CreateAuction(DateTimeOffset start) => new()
        {
            Uuid = Guid.NewGuid().ToString("N"),
            _start = start.ToUnixTimeMilliseconds(),
            _end = DateTimeOffset.UtcNow.AddHours(1).ToUnixTimeMilliseconds(),
            Bids = new(),
            ItemName = "Bread",
            ItemBytes = "H4sIAAAAAAAA/xXOSQrCQBQE0Ipj0gguPYNXyM4RFw7gBeSbfGNjD6H7C+ZE3sODie22oOqVAgpkWgHIFFTlbesdO4k5ZlY7rgLdpBTvjej2EqUzjNm9a/WLzSU+uqvx1aNMNesdeujpOptnGK7800ka7As1fRQ7XfPWUBMT81UY1zq2hroCg70PnP9xTD7v2+dtVqfD4XTMMTiSZeQpXAamGgrTzUsCLUSCvj6F08OkYbg8bxZrJHu0JksNpzH8APwCIpHWAAAA"
        };

        private class PageTestUpdater : Updater
        {
            public PageTestUpdater() : base(null, new NoopSkinHandler(), new ActivitySource("UpdaterTests"), null) { }

            public Task<int> SavePage(AuctionPage page, DateTime lastUpdate, FakeProducer producer)
            {
                return Save(page, lastUpdate, new AhStateSumary(), producer, default);
            }

            private class NoopSkinHandler : IItemSkinHandler
            {
                public void StoreIfNeeded(SaveAuction parsed, Auction auction) { }
            }
        }


        //[Test]
        public async Task Parse()
        {
            var update = new TestUpdater(null);

            var host = Program.CreateHostBuilder(new string[]{"--urls=http://localhost:6001/"}).Build();
            var cancleToken = new CancellationTokenSource(8000).Token;
            _ = host.RunAsync(cancleToken);
            await Task.Delay(200);
            // warmup
            await update.DoAnUpdate();

            var sw = Stopwatch.StartNew();
            await update.DoAnUpdate();

            var elapsed = sw.ElapsedMilliseconds;
            Assert.That(elapsed, Is.LessThanOrEqualTo(50));
            Assert.That(update.producer.lastAuction, Is.Not.Null);
            try 
            {
            await host.StopAsync();
            } catch(Exception e)
            {
                Console.WriteLine("failed to stop test host" + e);
            }
        }

        public class TestUpdater : NewUpdater
        {
            protected override string ApiBaseUrl => "http://localhost:6001";
            public FakeProducer producer = new FakeProducer();

            public TestUpdater(ActivitySource activitySource) : base(activitySource, null)
            {
            }

            public async Task DoAnUpdate()
            {

                await DoOneUpdate(DateTime.Now - TimeSpan.FromMinutes(1), producer, 0, null);
            }
        }

        public class FakeProducer : Confluent.Kafka.IProducer<string, Core.SaveAuction>
        {
            public SaveAuction lastAuction;
            public List<(string Topic, SaveAuction Auction)> Messages = new();
            public Handle Handle => throw new NotImplementedException();

            public string Name => throw new NotImplementedException();

            public void AbortTransaction(TimeSpan timeout)
            {
                throw new NotImplementedException();
            }

            public void AbortTransaction()
            {
                throw new NotImplementedException();
            }

            public int AddBrokers(string brokers)
            {
                throw new NotImplementedException();
            }

            public void BeginTransaction()
            {
                throw new NotImplementedException();
            }

            public void CommitTransaction(TimeSpan timeout)
            {
                throw new NotImplementedException();
            }

            public void CommitTransaction()
            {
                throw new NotImplementedException();
            }

            public void Dispose()
            {
                throw new NotImplementedException();
            }

            public int Flush(TimeSpan timeout)
            {
                return 1;
            }

            public void Flush(CancellationToken cancellationToken = default)
            {
                throw new NotImplementedException();
            }

            public void InitTransactions(TimeSpan timeout)
            {
                throw new NotImplementedException();
            }

            public int Poll(TimeSpan timeout)
            {
                throw new NotImplementedException();
            }

            public void Produce(string topic, Message<string, SaveAuction> message, Action<DeliveryReport<string, SaveAuction>> deliveryHandler = null)
            {
                lastAuction = message.Value;
                Messages.Add((topic, message.Value));
                Console.WriteLine("Produced: " + message.Value.Uuid);
            }

            public void Produce(TopicPartition topicPartition, Message<string, SaveAuction> message, Action<DeliveryReport<string, SaveAuction>> deliveryHandler = null)
            {
                
            }

            public Task<DeliveryResult<string, SaveAuction>> ProduceAsync(string topic, Message<string, SaveAuction> message, CancellationToken cancellationToken = default)
            {
                throw new NotImplementedException();
            }

            public Task<DeliveryResult<string, SaveAuction>> ProduceAsync(TopicPartition topicPartition, Message<string, SaveAuction> message, CancellationToken cancellationToken = default)
            {
                throw new NotImplementedException();
            }

            public void SendOffsetsToTransaction(IEnumerable<TopicPartitionOffset> offsets, IConsumerGroupMetadata groupMetadata, TimeSpan timeout)
            {
                throw new NotImplementedException();
            }

            public void SetSaslCredentials(string username, string password)
            {
                throw new NotImplementedException();
            }
        }
    }
}
