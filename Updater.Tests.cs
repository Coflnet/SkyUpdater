using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
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

        [Test]
        public void RepeatSalesUseItemUuidAcrossProducerPaths()
        {
            var updater = new TestUpdater(null);
            var itemUuid = Guid.NewGuid().ToString();
            var first = new SaveAuction
            {
                Uuid = Guid.NewGuid().ToString("N"),
                End = DateTime.UtcNow.AddMinutes(-1),
                FlatenedNBT = new() { { "uuid", itemUuid } }
            };
            var second = new SaveAuction
            {
                Uuid = Guid.NewGuid().ToString("N"),
                End = DateTime.UtcNow,
                FlatenedNBT = new() { { "uuid", itemUuid } }
            };

            updater.PublishSells(new() { first, second });
            Updater.ProduceIntoTopic(Updater.SoldAuctionsTopic, updater.producer, second, null);

            Assert.That(updater.producer.Messages.Select(m => m.Key), Is.EqualTo(new[] { itemUuid, itemUuid, itemUuid }));
            Assert.That(updater.producer.Messages.Select(m => m.Auction), Is.EqualTo(new[] { first, second, second }));
            Assert.That(updater.producer.Messages.Select(m => System.Text.Encoding.UTF8.GetString(m.Headers.GetLastBytes("deduplication-key"))),
                Is.EqualTo(new[] { first.Uuid, second.Uuid, second.Uuid }));
        }

        [Test]
        public void SoldAuctionWithoutItemUuidUsesAuctionUuid()
        {
            var updater = new TestUpdater(null);
            var auction = new SaveAuction { Uuid = Guid.NewGuid().ToString("N"), FlatenedNBT = new() };

            updater.PublishSells(new() { auction });
            Updater.ProduceIntoTopic(Updater.SoldAuctionsTopic, updater.producer, auction, null);

            Assert.That(updater.producer.Messages.Select(m => m.Key), Is.EqualTo(new[] { auction.Uuid, auction.Uuid }));
        }

        [Test]
        public void BidsKeepDistinctKeysWithoutDeduplicationHeader()
        {
            var producer = new FakeProducer();
            var auction = new SaveAuction
            {
                Uuid = Guid.NewGuid().ToString("N"),
                End = DateTime.UtcNow,
                Bids = new(),
                FlatenedNBT = new() { { "uuid", Guid.NewGuid().ToString() } }
            };
            Updater.ProduceIntoTopic(Updater.NewBidsTopic, producer, auction, null);
            auction.Bids.Add(new SaveBids());
            Updater.ProduceIntoTopic(Updater.NewBidsTopic, producer, auction, null);

            Assert.That(producer.Messages.Select(m => m.Key).Distinct().Count(), Is.EqualTo(2));
            Assert.That(producer.Messages.All(m => m.Headers == null), Is.True);
        }

        [Test]
        public void IfModifiedSinceIsOneSecondAfterThePreviousUpdateWasReceived()
        {
            var stamp = new DateTime(2026, 10, 12, 11, 25, 6, 275, DateTimeKind.Utc);
            var received = stamp.AddSeconds(4.9);

            Assert.That(NewUpdater.GetMinModifiedTime(stamp, received).UtcDateTime, Is.EqualTo(received.AddSeconds(1)));
        }

        /// <summary>
        /// The header used to be the stamp plus 64 seconds. Cloudflare sets Last-Modified to the second it fetched the snapshot,
        /// so every snapshot fetched less than 64.7 seconds after the previous stamp answered 304 until the poll timed out
        /// </summary>
        [TestCase(4.2)]
        [TestCase(4.5)]
        [TestCase(4.9)]
        [TestCase(5.6)]
        [TestCase(15)]
        public void NextSnapshotIsNeverAnsweredAsNotModified(double fetchedAfterStamp)
        {
            var stamp = new DateTime(2026, 10, 12, 11, 25, 6, 275, DateTimeKind.Utc);
            foreach (var receivedAfterStamp in new[] { 4.2, 4.9, 5.6, 15, 28.3, 58, 64.9, 120 })
            {
                var ifModifiedSince = HttpDate(NewUpdater.GetMinModifiedTime(stamp, stamp.AddSeconds(receivedAfterStamp)));
                var nextLastModified = HttpDate(new DateTimeOffset(stamp.AddSeconds(60 + fetchedAfterStamp)));

                // the server answers 304 when Last-Modified is not later than If-Modified-Since
                Assert.That(nextLastModified, Is.GreaterThan(ifModifiedSince), $"received {receivedAfterStamp}s after the stamp");
            }
        }

        [Test]
        public void SnapshotWeAlreadyHaveIsAnsweredAsNotModified()
        {
            var stamp = new DateTime(2026, 10, 12, 11, 25, 6, 275, DateTimeKind.Utc);
            // fetched by cloudflare at the latest when we received it
            var received = stamp.AddSeconds(4.9);

            Assert.That(HttpDate(new DateTimeOffset(received)), Is.LessThanOrEqualTo(HttpDate(NewUpdater.GetMinModifiedTime(stamp, received))));
        }

        [Test]
        public void IfModifiedSinceFallsBackToTheStampWhenNothingWasReceived()
        {
            var stamp = new DateTime(2026, 10, 12, 11, 25, 6, 275, DateTimeKind.Utc);

            Assert.That(NewUpdater.GetMinModifiedTime(stamp, default).UtcDateTime, Is.EqualTo(stamp.AddSeconds(1)));
        }

        /// <summary>
        /// Seen after the first rollout: the page takes up to 7 seconds to arrive and cloudflare stamps the cached copy when it is complete.
        /// With the time of the first byte as reference every poll of the next minute got the full old page instead of a 304
        /// </summary>
        [TestCase(0.8)]
        [TestCase(2.7)]
        [TestCase(7.2)]
        public void CopyStoredWhenThePageWasCompleteIsAnsweredAsNotModified(double downloadSeconds)
        {
            var stamp = new DateTime(2026, 10, 12, 13, 37, 6, 275, DateTimeKind.Utc);
            var finished = stamp.AddSeconds(5.1 + downloadSeconds);

            Assert.That(HttpDate(new DateTimeOffset(finished)), Is.LessThanOrEqualTo(HttpDate(NewUpdater.GetMinModifiedTime(stamp, finished))));
        }

        [Test]
        public void OldCopyStoredLaterBecomesTheValidator()
        {
            var stamp = new DateTime(2026, 10, 12, 13, 37, 6, 275, DateTimeKind.Utc);
            var current = NewUpdater.GetMinModifiedTime(stamp, stamp.AddSeconds(5.1));
            var storedAgain = new DateTimeOffset(stamp.AddSeconds(21));

            var validator = NewUpdater.NewerValidator(current, storedAgain, stamp);

            Assert.That(validator, Is.EqualTo(storedAgain));
            // an older or missing header keeps what we had
            Assert.That(NewUpdater.NewerValidator(validator, new DateTimeOffset(stamp.AddSeconds(8)), stamp), Is.EqualTo(validator));
            Assert.That(NewUpdater.NewerValidator(validator, null, stamp), Is.EqualTo(validator));
        }

        [Test]
        public void ValidatorOfAnOldCopyNeverReachesTheNextSnapshot()
        {
            var stamp = new DateTime(2026, 10, 12, 13, 37, 6, 275, DateTimeKind.Utc);
            var current = NewUpdater.GetMinModifiedTime(stamp, stamp.AddSeconds(5.1));
            // the old snapshot was stored again in the same second the next one gets fetched
            var storedAgain = new DateTimeOffset(stamp.AddSeconds(64.1));
            var nextLastModified = HttpDate(new DateTimeOffset(stamp.AddSeconds(64.6)));

            var validator = NewUpdater.NewerValidator(current, storedAgain, stamp);

            Assert.That(nextLastModified, Is.GreaterThan(HttpDate(validator)));
        }

        /// <summary>
        /// The first rollout took the arrival of the first byte as the time the update was received. The page streams for
        /// seconds and the cached copy is stamped at its end, so the next minute got the full old page on every poll
        /// </summary>
        [Test]
        public async Task NextPollAsksForNewerThanTheEndOfThePreviousDownload()
        {
            var updater = new SlowPageUpdater();
            var stamp = DateTimeOffset.FromUnixTimeMilliseconds(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - 5000);

            var update = await updater.Download(stamp, TimeSpan.FromSeconds(2.2));
            var finished = DateTimeOffset.UtcNow;
            Assert.That(update, Is.EqualTo(stamp.LocalDateTime));

            var afterTimeout = await updater.PollWithoutNewPage(update);

            Assert.That(updater.IfModifiedSince, Is.GreaterThanOrEqualTo(finished));
            Assert.That(updater.IfModifiedSince, Is.LessThan(finished.AddSeconds(2)));
            // a poll that ends without a page keeps the last update
            Assert.That(afterTimeout, Is.EqualTo(update));
        }

        private class SlowPageUpdater : NewUpdater
        {
            private Func<CancellationTokenSource, HttpResponseMessage> respond;
            private CancellationTokenSource pollToken;
            public DateTimeOffset? IfModifiedSince;

            public SlowPageUpdater() : base(new ActivitySource("UpdaterTests"), null) { }

            protected override IProducer<string, SaveAuction> GetProducer() => new FakeProducer();

            protected override Task<HttpResponseMessage> NewMethod(CancellationTokenSource tokenSource, HttpRequestMessage message)
            {
                IfModifiedSince = message.Headers.IfModifiedSince;
                return Task.FromResult(respond(pollToken));
            }

            public async Task<DateTime> Download(DateTimeOffset stamp, TimeSpan downloadTime)
            {
                var head = System.Text.Encoding.UTF8.GetBytes($"{{\"success\":true,\"page\":0,\"totalPages\":1,\"totalAuctions\":0,\"lastUpdated\":{stamp.ToUnixTimeMilliseconds()},\"auctions\":[");
                var tail = System.Text.Encoding.UTF8.GetBytes("]}");
                respond = _ =>
                {
                    var response = new HttpResponseMessage(HttpStatusCode.OK) { Content = new StreamContent(new SlowStream(head, tail, downloadTime)) };
                    response.Headers.Date = DateTimeOffset.UtcNow;
                    return response;
                };
                return await Poll(stamp.LocalDateTime.AddSeconds(-60));
            }

            public async Task<DateTime> PollWithoutNewPage(DateTime lastUpdate)
            {
                respond = token =>
                {
                    // one answer is enough, end the poll like its timeout would
                    token.Cancel();
                    return new HttpResponseMessage(HttpStatusCode.NotModified);
                };
                return await Poll(lastUpdate);
            }

            private async Task<DateTime> Poll(DateTime lastUpdate)
            {
                pollToken = new CancellationTokenSource();
                var result = await GetAndSavePage(0, new FakeProducer(), lastUpdate, new Activity("test"), pollToken, 0);
                return result.Item1;
            }

            /// <summary>
            /// Hands out the start of the page at once and the rest after a delay, like a page still being sent by the origin
            /// </summary>
            private class SlowStream : Stream
            {
                private readonly byte[] head;
                private readonly byte[] tail;
                private readonly TimeSpan delay;
                private int step;

                public SlowStream(byte[] head, byte[] tail, TimeSpan delay)
                {
                    this.head = head;
                    this.tail = tail;
                    this.delay = delay;
                }

                public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
                {
                    step++;
                    if (step == 1)
                    {
                        head.CopyTo(buffer);
                        return head.Length;
                    }
                    if (step == 2)
                    {
                        await Task.Delay(delay, cancellationToken);
                        tail.CopyTo(buffer);
                        return tail.Length;
                    }
                    return 0;
                }

                public override int Read(byte[] buffer, int offset, int count) => ReadAsync(buffer.AsMemory(offset, count)).AsTask().Result;
                public override bool CanRead => true;
                public override bool CanSeek => false;
                public override bool CanWrite => false;
                public override long Length => throw new NotSupportedException();
                public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
                public override void Flush() { }
                public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
                public override void SetLength(long value) => throw new NotSupportedException();
                public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
            }
        }

        [Test]
        public void TimedOutPollKeepsTheLastUpdate()
        {
            var lastUpdate = new DateTime(2026, 10, 12, 11, 25, 6, 275, DateTimeKind.Utc).ToLocalTime();
            // no page arrived, the page reports the unix epoch
            var emptyPage = new AuctionPage().LastUpdated;

            Assert.That(NewUpdater.LatestUpdate(emptyPage, lastUpdate), Is.EqualTo(lastUpdate));
            Assert.That(NewUpdater.LatestUpdate(lastUpdate.AddSeconds(60), lastUpdate), Is.EqualTo(lastUpdate.AddSeconds(60)));
        }

        /// <summary>
        /// Http dates have second resolution
        /// </summary>
        private static DateTimeOffset HttpDate(DateTimeOffset time) => DateTimeOffset.Parse(time.ToString("r"));

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

            protected override IProducer<string, SaveAuction> GetProducer() => producer;

            public void PublishSells(List<SaveAuction> auctions) => ProduceSells(auctions);

            public async Task DoAnUpdate()
            {

                await DoOneUpdate(DateTime.Now - TimeSpan.FromMinutes(1), producer, 0, null);
            }
        }

        public class FakeProducer : Confluent.Kafka.IProducer<string, Core.SaveAuction>
        {
            public SaveAuction lastAuction;
            public List<(string Topic, SaveAuction Auction, string Key, Headers Headers)> Messages = new();
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
                Messages.Add((topic, message.Value, message.Key, message.Headers));
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
