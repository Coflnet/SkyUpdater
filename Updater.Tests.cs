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

        /// <summary>
        /// Not modified answers and old snapshot pages used one counter, so an old page after many polls slept for the
        /// count of all polls times 10ms and the new snapshot was missed
        /// </summary>
        [Test]
        public async Task OldSnapshotPageAfterManyPollsBacksOffBriefly()
        {
            var updater = new ScriptedPageUpdater();
            var lastUpdate = DateTimeOffset.FromUnixTimeMilliseconds(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - 3000);
            var script = Enumerable.Range(0, 45).Select(_ => updater.NotModifiedAnswer()).ToList();
            script.Add(updater.Snapshot(lastUpdate, lastUpdate.AddSeconds(65)));
            var oldIndex = script.Count - 1;
            script.Add(updater.Snapshot(lastUpdate.AddSeconds(60), null, CreateAuction(lastUpdate.AddSeconds(30))));

            var result = await updater.Run(lastUpdate.LocalDateTime, script);

            Assert.That(result, Is.EqualTo(lastUpdate.AddSeconds(60).LocalDateTime));
            Assert.That(updater.Arrivals.Count, Is.EqualTo(script.Count));
            var wait = updater.Arrivals[oldIndex + 1] - updater.HandedOut[oldIndex];
            Assert.That(wait, Is.LessThan(TimeSpan.FromMilliseconds(150)));
        }

        [Test]
        public async Task OldSnapshotPageAfterMoreThanFiftyPollsDoesNotGiveUp()
        {
            var updater = new ScriptedPageUpdater();
            var lastUpdate = DateTimeOffset.FromUnixTimeMilliseconds(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - 3000);
            var script = Enumerable.Range(0, 55).Select(_ => updater.NotModifiedAnswer()).ToList();
            script.Add(updater.Snapshot(lastUpdate, lastUpdate.AddSeconds(65)));
            var newAuction = CreateAuction(lastUpdate.AddSeconds(30));
            script.Add(updater.Snapshot(lastUpdate.AddSeconds(60), null, newAuction));

            var result = await updater.Run(lastUpdate.LocalDateTime, script);

            Assert.That(result, Is.EqualTo(lastUpdate.AddSeconds(60).LocalDateTime));
            Assert.That(updater.Found, Is.EqualTo(new[] { newAuction.Uuid }));
        }

        /// <summary>
        /// Hypixel lists an auction 20 seconds after it started, so a snapshot brings auctions that started before the
        /// previous stamp. They were dropped by the start time check and never evaluated.
        /// </summary>
        [Test]
        public async Task AuctionStartedShortlyBeforePreviousStampIsFound()
        {
            var updater = new ScriptedPageUpdater();
            var lastUpdate = DateTimeOffset.FromUnixTimeMilliseconds(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - 3000);
            var late = CreateAuction(lastUpdate.AddSeconds(-15));

            await updater.Run(lastUpdate.LocalDateTime, new() { updater.Snapshot(lastUpdate.AddSeconds(60), null, late) });

            Assert.That(updater.Found, Is.EqualTo(new[] { late.Uuid }));
        }

        [Test]
        public async Task AuctionSeenInThePreviousSnapshotIsNotHandedOverAgain()
        {
            var updater = new ScriptedPageUpdater();
            var lastUpdate = DateTimeOffset.FromUnixTimeMilliseconds(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - 3000);
            var late = CreateAuction(lastUpdate.AddSeconds(-15));

            await updater.Run(lastUpdate.LocalDateTime, new() { updater.Snapshot(lastUpdate.AddSeconds(60), null, late) });
            await updater.Run(lastUpdate.AddSeconds(60).LocalDateTime, new() { updater.Snapshot(lastUpdate.AddSeconds(120), null, late) });

            Assert.That(updater.Found, Is.EqualTo(new[] { late.Uuid }));
        }

        [Test]
        public async Task AuctionStartedLongBeforePreviousStampIsNotHandedOver()
        {
            var updater = new ScriptedPageUpdater();
            var lastUpdate = DateTimeOffset.FromUnixTimeMilliseconds(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - 3000);
            var old = CreateAuction(lastUpdate.AddSeconds(-40));

            await updater.Run(lastUpdate.LocalDateTime, new() { updater.Snapshot(lastUpdate.AddSeconds(60), null, old) });

            Assert.That(updater.Found, Is.Empty);
        }

        private class SellsRecordingUpdater : NewUpdater
        {
            public List<List<SaveAuction>> Produced = new();
            public SellsRecordingUpdater() : base(new ActivitySource("UpdaterTests"), null) { }
            protected override void ProduceSells(List<SaveAuction> binupdate) => Produced.Add(binupdate);
            public Task<bool> Call(Task<List<SaveAuction>> download, TimeSpan timeLeft) => UpdateSells(download, timeLeft);
        }

        [Test]
        public async Task HangingSoldAuctionsDownloadDoesNotHoldUpTheLoop()
        {
            var updater = new SellsRecordingUpdater();
            var hanging = new TaskCompletionSource<List<SaveAuction>>();
            var unobserved = new List<Exception>();
            EventHandler<UnobservedTaskExceptionEventArgs> handler = (_, e) => unobserved.Add(e.Exception);
            TaskScheduler.UnobservedTaskException += handler;
            try
            {
                var watch = Stopwatch.StartNew();
                var produced = await updater.Call(hanging.Task, TimeSpan.FromMilliseconds(100));
                Assert.That(produced, Is.False);
                Assert.That(watch.Elapsed, Is.LessThan(TimeSpan.FromSeconds(1)));
                Assert.That(updater.Produced, Is.Empty);

                hanging.SetException(new InvalidOperationException("late failure"));
                await Task.Delay(100);
                GC.Collect();
                GC.WaitForPendingFinalizers();
                GC.Collect();
                GC.WaitForPendingFinalizers();
                Assert.That(unobserved, Is.Empty);
            }
            finally
            {
                TaskScheduler.UnobservedTaskException -= handler;
            }
        }

        [Test]
        public async Task SoldAuctionsInTimeAreProduced()
        {
            var updater = new SellsRecordingUpdater();
            var download = Task.Run(async () =>
            {
                await Task.Delay(20);
                return new List<SaveAuction> { new() { Uuid = "a" }, new() { Uuid = "b" } };
            });
            var produced = await updater.Call(download, TimeSpan.FromSeconds(2));
            Assert.That(produced, Is.True);
            Assert.That(updater.Produced.Single().Select(a => a.Uuid), Is.EqualTo(new[] { "a", "b" }));
        }

        [Test]
        public async Task SoldAuctionsAfterALateSnapshotAreSkippedAtOnce()
        {
            var updater = new SellsRecordingUpdater();
            var watch = Stopwatch.StartNew();
            var produced = await updater.Call(new TaskCompletionSource<List<SaveAuction>>().Task, TimeSpan.FromSeconds(-5));
            Assert.That(produced, Is.False);
            Assert.That(watch.Elapsed, Is.LessThan(TimeSpan.FromMilliseconds(200)));
            Assert.That(updater.Produced, Is.Empty);
        }

        [Test]
        public void FailingSoldAuctionsDownloadStillThrows()
        {
            var updater = new SellsRecordingUpdater();
            var failing = Task.FromException<List<SaveAuction>>(new ArgumentNullException("content"));
            Assert.ThrowsAsync<ArgumentNullException>(() => updater.Call(failing, TimeSpan.FromSeconds(2)));
            Assert.That(updater.Produced, Is.Empty);
        }

        /// <summary>
        /// Serves a fixed sequence of answers and notes when each request arrived and when its answer was handed out
        /// </summary>
        private class ScriptedPageUpdater : NewUpdater
        {
            private List<Func<HttpResponseMessage>> script;
            private CancellationTokenSource pollToken;
            private readonly Stopwatch clock = Stopwatch.StartNew();
            public List<TimeSpan> Arrivals = new();
            public List<TimeSpan> HandedOut = new();
            public List<string> Found = new();

            public ScriptedPageUpdater() : base(new ActivitySource("UpdaterTests"), null) { }

            protected override IProducer<string, SaveAuction> GetProducer() => new FakeProducer();

            protected override void FoundNew(int pageId, IProducer<string, SaveAuction> p, AuctionPage page, int tryCount, Auction auction, Activity prodSpan, int count)
            {
                Found.Add(auction.Uuid);
            }

            protected override Task<HttpResponseMessage> NewMethod(CancellationTokenSource tokenSource, HttpRequestMessage message, HttpClient client)
            {
                Arrivals.Add(clock.Elapsed);
                var index = Arrivals.Count - 1;
                if (index >= script.Count)
                {
                    // out of answers, end the poll like its timeout would
                    pollToken.Cancel();
                    HandedOut.Add(clock.Elapsed);
                    return Task.FromResult(NotModified());
                }
                var response = script[index]();
                HandedOut.Add(clock.Elapsed);
                return Task.FromResult(response);
            }

            public HttpResponseMessage NotModified() => new(HttpStatusCode.NotModified);
            public Func<HttpResponseMessage> NotModifiedAnswer() => NotModified;

            public Func<HttpResponseMessage> Snapshot(DateTimeOffset stamp, DateTimeOffset? lastModified, params Auction[] auctions) => () =>
            {
                var json = $"{{\"success\":true,\"page\":0,\"totalPages\":1,\"totalAuctions\":{auctions.Length},\"lastUpdated\":{stamp.ToUnixTimeMilliseconds()},\"auctions\":[{string.Join(",", auctions.Select(a => System.Text.Json.JsonSerializer.Serialize(a)))}]}}";
                var response = new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent(json) };
                // the code treats a missing date like the missing validator as not modified
                response.Headers.Date = DateTimeOffset.UtcNow;
                response.Content.Headers.LastModified = lastModified;
                return response;
            };

            public async Task<DateTime> Run(DateTime lastUpdate, List<Func<HttpResponseMessage>> answers)
            {
                script = answers;
                Arrivals.Clear();
                HandedOut.Clear();
                pollToken = new CancellationTokenSource();
                var result = await GetAndSavePage(0, new FakeProducer(), lastUpdate, new Activity("test"), pollToken, 0);
                return result.Item1;
            }
        }

        private class SlowPageUpdater : NewUpdater
        {
            private Func<CancellationTokenSource, HttpResponseMessage> respond;
            private CancellationTokenSource pollToken;
            public DateTimeOffset? IfModifiedSince;

            public SlowPageUpdater() : base(new ActivitySource("UpdaterTests"), null) { }

            protected override IProducer<string, SaveAuction> GetProducer() => new FakeProducer();

            protected override Task<HttpResponseMessage> NewMethod(CancellationTokenSource tokenSource, HttpRequestMessage message, HttpClient client)
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

        /// <summary>
        /// Serves one scripted body per request, in the order the requests arrive, and notes when each auction was handed over.
        /// A body is a list of chunks, each handed out after its own delay (infinite stalls the body).
        /// </summary>
        private class RacingPageUpdater : NewUpdater
        {
            private readonly Queue<(TimeSpan headerDelay, (byte[] data, TimeSpan delay)[] chunks)> bodies = new();
            private readonly Stopwatch clock = Stopwatch.StartNew();
            public readonly System.Collections.Concurrent.ConcurrentQueue<(string uuid, TimeSpan at)> Found = new();
            public int Requests;
            public PageConnections Mode = PageConnections.Shared;
            public Func<int, int, int, string> UrlOverride;
            public Func<int, TimeSpan?> DelayOverride;
            public readonly Queue<HttpStatusCode> Statuses = new();
            public string UrlOf(int pageId, int iter, int tryCount) => PageUrl(pageId, iter, tryCount);
            public TimeSpan DelayOf(int iter) => StartDelay(iter);
            protected override string PageUrl(int pageId, int iter, int tryCount) => UrlOverride?.Invoke(pageId, iter, tryCount) ?? base.PageUrl(pageId, iter, tryCount);
            protected override TimeSpan StartDelay(int iter) => DelayOverride?.Invoke(iter) ?? base.StartDelay(iter);
            /// <summary>
            /// The client each request was sent with, by download id (the id is in the url: none, page only or page with t)
            /// </summary>
            public readonly System.Collections.Concurrent.ConcurrentQueue<(string url, HttpClient client)> Clients = new();

            private protected override PageConnections Connections => Mode;
            public PageRace StartRace() => NewRace();

            public RacingPageUpdater() : base(new ActivitySource("UpdaterTests"), null) { }

            protected override IProducer<string, SaveAuction> GetProducer() => new FakeProducer();

            protected override void FoundNew(int pageId, IProducer<string, SaveAuction> p, AuctionPage page, int tryCount, Auction auction, Activity prodSpan, int count)
            {
                Found.Enqueue((auction.Uuid, clock.Elapsed));
            }

            public TimeSpan Now => clock.Elapsed;

            private static byte[] Bytes(string text) => System.Text.Encoding.UTF8.GetBytes(text);
            private static string Json(Auction a) => System.Text.Json.JsonSerializer.Serialize(a);

            /// <summary>
            /// Queues a body: the start of the page with the first auctions, then the rest after the pause
            /// </summary>
            public void AddBody(TimeSpan headerDelay, DateTimeOffset stamp, Auction[] first, TimeSpan pause, Auction[] rest)
            {
                var head = $"{{\"success\":true,\"page\":0,\"totalPages\":1,\"totalAuctions\":0,\"lastUpdated\":{stamp.ToUnixTimeMilliseconds()},\"auctions\":[{string.Join(",", first.Select(Json))}";
                var chunks = new List<(byte[], TimeSpan)> { (Bytes(head), TimeSpan.Zero) };
                if (rest != null)
                    chunks.Add((Bytes((first.Length > 0 ? "," : "") + string.Join(",", rest.Select(Json)) + "]}"), pause));
                else
                    chunks.Add((Bytes("]}"), pause));
                bodies.Enqueue((headerDelay, chunks.ToArray()));
            }

            /// <summary>
            /// Queues a body that sends the start of the page and nothing after it
            /// </summary>
            public void AddStalledBody(TimeSpan headerDelay, DateTimeOffset stamp)
            {
                var head = $"{{\"success\":true,\"page\":0,\"totalPages\":1,\"totalAuctions\":0,\"lastUpdated\":{stamp.ToUnixTimeMilliseconds()},\"auctions\":[";
                bodies.Enqueue((headerDelay, new[] { (Bytes(head), TimeSpan.Zero), (Bytes("]}"), Timeout.InfiniteTimeSpan) }));
            }

            /// <summary>
            /// Queues a body that sends the start of the page with the given auctions and then fails the next read like a reset connection
            /// </summary>
            public void AddBreakingBody(TimeSpan headerDelay, DateTimeOffset stamp, params Auction[] first)
            {
                var head = $"{{\"success\":true,\"page\":0,\"totalPages\":1,\"totalAuctions\":0,\"lastUpdated\":{stamp.ToUnixTimeMilliseconds()},\"auctions\":[{string.Join(",", first.Select(Json))}";
                bodies.Enqueue((headerDelay, new[] { (Bytes(head), TimeSpan.Zero), (Array.Empty<byte>(), TimeSpan.MinValue) }));
            }

            public Task<(DateTime, int)[]> Await(NewUpdater.PageRace race, params Task<(DateTime, int)>[] downloads) => AwaitRace(race, downloads);

            protected override async Task<HttpResponseMessage> NewMethod(CancellationTokenSource tokenSource, HttpRequestMessage message, HttpClient client)
            {
                (TimeSpan headerDelay, (byte[] data, TimeSpan delay)[] chunks) body;
                lock (bodies)
                {
                    Requests++;
                    Clients.Enqueue((message.RequestUri.ToString(), client));
                    if (Statuses.TryDequeue(out var status))
                    {
                        var plain = new HttpResponseMessage(status);
                        plain.Headers.Date = DateTimeOffset.UtcNow;
                        return plain;
                    }
                    if (!bodies.TryDequeue(out body))
                        throw new InvalidOperationException("more requests than scripted bodies");
                }
                await Task.Delay(body.headerDelay, tokenSource.Token);
                var response = new HttpResponseMessage(HttpStatusCode.OK) { Content = new StreamContent(new ChunkStream(body.chunks)) };
                response.Headers.Date = DateTimeOffset.UtcNow;
                return response;
            }

            public Task<(DateTime, int)> Download(DateTime lastUpdate, CancellationTokenSource round, int iter, PageRace race)
            {
                return GetAndSavePage(0, new FakeProducer(), lastUpdate, new Activity("test"), round, iter, race);
            }

            private class ChunkStream : Stream
            {
                private readonly (byte[] data, TimeSpan delay)[] chunks;
                private int step;

                public ChunkStream((byte[] data, TimeSpan delay)[] chunks) => this.chunks = chunks;

                public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
                {
                    if (step >= chunks.Length)
                        return 0;
                    var (data, delay) = chunks[step++];
                    if (delay == TimeSpan.MinValue)
                        throw new IOException("connection reset");
                    if (delay != TimeSpan.Zero)
                        await Task.Delay(delay, cancellationToken);
                    data.CopyTo(buffer);
                    return data.Length;
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

        /// <summary>
        /// The first download that read a new snapshot used to cancel the other ones, so a body that delivers faster than the one
        /// whose headers arrived first was never read. With a race both bodies are read and each auction comes from whichever has it first.
        /// </summary>
        [Test]
        public async Task RacingBodiesHandOverEachAuctionOnceFromTheFasterBody()
        {
            var updater = new RacingPageUpdater();
            var lastUpdate = DateTimeOffset.FromUnixTimeMilliseconds(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - 3000);
            var stamp = lastUpdate.AddSeconds(60);
            var x = CreateAuction(lastUpdate);
            var y = CreateAuction(lastUpdate);
            // A has its headers first but pauses before the second auction, B is a little later and has both at once
            updater.AddBody(TimeSpan.Zero, stamp, new[] { x }, TimeSpan.FromSeconds(2), new[] { y });
            updater.AddBody(TimeSpan.Zero, stamp, new[] { x, y }, TimeSpan.Zero, null);
            var round = new CancellationTokenSource(20000);
            var race = new NewUpdater.PageRace();
            var started = updater.Now;

            var results = await Task.WhenAll(
                updater.Download(lastUpdate.LocalDateTime, round, 0, race),
                updater.Download(lastUpdate.LocalDateTime, round, 1, race));

            var found = updater.Found.ToList();
            Assert.That(found.Select(f => f.uuid), Is.EquivalentTo(new[] { x.Uuid, y.Uuid }));
            Assert.That(found.Single(f => f.uuid == y.Uuid).at - started, Is.LessThan(TimeSpan.FromSeconds(1)));
            Assert.That(updater.Now - started, Is.LessThan(TimeSpan.FromSeconds(1.5)), "the paused body was waited for");
            Assert.That(results.Select(r => r.Item1), Is.All.EqualTo(stamp.LocalDateTime));
        }

        [Test]
        public async Task RacingBodyThatStallsIsCutWhenAnotherOneIsDone()
        {
            var updater = new RacingPageUpdater();
            var lastUpdate = DateTimeOffset.FromUnixTimeMilliseconds(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - 3000);
            var stamp = lastUpdate.AddSeconds(60);
            var x = CreateAuction(lastUpdate);
            // A is slow enough for B to get its start, B never delivers anything after it
            updater.AddBody(TimeSpan.Zero, stamp, new[] { x }, TimeSpan.FromMilliseconds(500), null);
            updater.AddStalledBody(TimeSpan.Zero, stamp);
            var round = new CancellationTokenSource(20000);
            var race = new NewUpdater.PageRace();
            var started = updater.Now;

            var results = await Task.WhenAll(
                updater.Download(lastUpdate.LocalDateTime, round, 0, race),
                updater.Download(lastUpdate.LocalDateTime, round, 1, race)).WaitAsync(TimeSpan.FromSeconds(5));

            Assert.That(updater.Now - started, Is.LessThan(TimeSpan.FromSeconds(1.5)));
            Assert.That(results.Select(r => r.Item1), Is.All.EqualTo(stamp.LocalDateTime));
            Assert.That(updater.Found.Select(f => f.uuid), Is.EqualTo(new[] { x.Uuid }));
        }

        [Test]
        public async Task RacingBodyThatBreaksDoesNotFailARoundAnotherOneFinished()
        {
            var updater = new RacingPageUpdater();
            var lastUpdate = DateTimeOffset.FromUnixTimeMilliseconds(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - 3000);
            var stamp = lastUpdate.AddSeconds(60);
            var x = CreateAuction(lastUpdate);
            var y = CreateAuction(lastUpdate);
            // A hands over X and breaks, B is later and completes
            updater.AddBreakingBody(TimeSpan.Zero, stamp, x);
            updater.AddBody(TimeSpan.FromMilliseconds(300), stamp, new[] { x, y }, TimeSpan.Zero, null);
            var round = new CancellationTokenSource(20000);
            var race = new NewUpdater.PageRace();

            var results = await updater.Await(race,
                updater.Download(lastUpdate.LocalDateTime, round, 0, race),
                updater.Download(lastUpdate.LocalDateTime, round, 1, race));

            Assert.That(updater.Found.Select(f => f.uuid), Is.EquivalentTo(new[] { x.Uuid, y.Uuid }));
            Assert.That(results.Max(r => r.Item1), Is.EqualTo(stamp.LocalDateTime));
            Assert.That(race.IsFinished, Is.True);
        }

        [Test]
        public async Task RacingBodyThatBreaksFailsTheRoundWhenNoOtherOneFinishes()
        {
            var updater = new RacingPageUpdater();
            var lastUpdate = DateTimeOffset.FromUnixTimeMilliseconds(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - 3000);
            var stamp = lastUpdate.AddSeconds(60);
            updater.AddBreakingBody(TimeSpan.Zero, stamp, CreateAuction(lastUpdate));
            var round = new CancellationTokenSource(20000);
            var race = new NewUpdater.PageRace();

            Assert.ThrowsAsync<IOException>(async () => await updater.Await(race, updater.Download(lastUpdate.LocalDateTime, round, 0, race)));
            Assert.That(race.IsFinished, Is.False);
        }

        [Test]
        public async Task WithoutARaceTheSecondDownloadIsCancelledByTheFirst()
        {
            var updater = new RacingPageUpdater();
            var lastUpdate = DateTimeOffset.FromUnixTimeMilliseconds(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - 3000);
            var stamp = lastUpdate.AddSeconds(60);
            var x = CreateAuction(lastUpdate);
            var y = CreateAuction(lastUpdate);
            updater.AddBody(TimeSpan.Zero, stamp, new[] { x }, TimeSpan.FromMilliseconds(500), new[] { y });
            updater.AddBody(TimeSpan.Zero, stamp, new[] { x, y }, TimeSpan.Zero, null);
            var round = new CancellationTokenSource(20000);

            var first = updater.Download(lastUpdate.LocalDateTime, round, 0, null);
            var second = updater.Download(lastUpdate.LocalDateTime, round, 1, null);
            var results = await Task.WhenAll(first, second);

            Assert.That(results[0].Item1, Is.EqualTo(stamp.LocalDateTime));
            Assert.That(results[1].Item1, Is.EqualTo(lastUpdate.LocalDateTime));
            Assert.That(updater.Requests, Is.EqualTo(1));
            // everything came from the first body, in its order
            Assert.That(updater.Found.Select(f => f.uuid), Is.EqualTo(new[] { x.Uuid, y.Uuid }));
        }

        /// <summary>
        /// Runs one round of three downloads that each see the new snapshot, returns the race
        /// </summary>
        private static async Task<NewUpdater.PageRace> RunThreeDownloads(RacingPageUpdater updater, DateTimeOffset lastUpdate, TimeSpan pause)
        {
            var stamp = lastUpdate.AddSeconds(60);
            var x = CreateAuction(lastUpdate);
            var y = CreateAuction(lastUpdate);
            for (var i = 0; i < 3; i++)
                updater.AddBody(TimeSpan.Zero, stamp, new[] { x }, pause, new[] { y });
            var round = new CancellationTokenSource(20000);
            var race = updater.StartRace();
            await updater.Await(race,
                updater.Download(lastUpdate.LocalDateTime, round, 0, race),
                updater.Download(lastUpdate.LocalDateTime, round, 1, race),
                updater.Download(lastUpdate.LocalDateTime, round, 2, race));
            return race;
        }

        [Test]
        public void DefaultPageUrlsAreUnchanged()
        {
            var updater = new RacingPageUpdater();

            Assert.That(updater.UrlOf(0, 0, 0), Is.EqualTo("https://api.hypixel.net/v2/skyblock/auctions"));
            Assert.That(updater.UrlOf(0, 2, 7), Is.EqualTo("https://api.hypixel.net/v2/skyblock/auctions?page=0"));
            Assert.That(updater.UrlOf(3, 0, 0), Is.EqualTo("https://api.hypixel.net/v2/skyblock/auctions?page=3"));
            var fourth = updater.UrlOf(0, 4, 7);
            Assert.That(fourth, Does.StartWith("https://api.hypixel.net/v2/skyblock/auctions?page=0&t="));
            Assert.That(fourth, Does.Contain("&t=7"));
        }

        [Test]
        public void DefaultStartDelayIs200MsPerId()
        {
            var updater = new RacingPageUpdater();

            Assert.That(updater.DelayOf(0), Is.EqualTo(TimeSpan.Zero));
            Assert.That(updater.DelayOf(2), Is.EqualTo(TimeSpan.FromMilliseconds(400)));
            Assert.That(updater.DelayOf(4), Is.EqualTo(TimeSpan.FromMilliseconds(800)));
        }

        [Test]
        public async Task OverriddenUrlIsRequestedAndOverriddenDelayHonoured()
        {
            var updater = new RacingPageUpdater
            {
                UrlOverride = (page, iter, tries) => "https://example.invalid/custom/" + iter,
                DelayOverride = iter => TimeSpan.FromMilliseconds(300)
            };
            var lastUpdate = RecentStamp();
            updater.AddBody(TimeSpan.Zero, lastUpdate.AddSeconds(60), new[] { CreateAuction(lastUpdate) }, TimeSpan.Zero, null);
            var round = new CancellationTokenSource(20000);
            var race = updater.StartRace();
            var started = updater.Now;

            await updater.Download(lastUpdate.LocalDateTime, round, 3, race);

            Assert.That(updater.Clients.Select(c => c.url), Is.EqualTo(new[] { "https://example.invalid/custom/3" }));
            Assert.That(updater.Now - started, Is.GreaterThanOrEqualTo(TimeSpan.FromMilliseconds(290)));
        }

        [Test]
        public async Task RaceCountsResponsesAndUnexpectedStatuses()
        {
            var updater = new RacingPageUpdater();
            var lastUpdate = RecentStamp();
            updater.Statuses.Enqueue(HttpStatusCode.NotModified);
            updater.Statuses.Enqueue(HttpStatusCode.TooManyRequests);
            updater.AddBody(TimeSpan.Zero, lastUpdate.AddSeconds(60), new[] { CreateAuction(lastUpdate) }, TimeSpan.Zero, null);
            var round = new CancellationTokenSource(20000);
            var race = updater.StartRace();

            await updater.Download(lastUpdate.LocalDateTime, round, 0, race);

            Assert.That(race.Requests, Is.EqualTo(3));
            Assert.That(race.OtherAnswers, Is.EqualTo(1));
        }

        private static DateTimeOffset RecentStamp() => DateTimeOffset.FromUnixTimeMilliseconds(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - 3000);

        [Test]
        public async Task OwnConnectionsGiveEveryDownloadItsOwnClientAndKeepItBetweenRounds()
        {
            var updater = new RacingPageUpdater { Mode = NewUpdater.PageConnections.Separate };
            var first = RecentStamp();

            var race = await RunThreeDownloads(updater, first, TimeSpan.FromMilliseconds(600));
            var firstRound = updater.Clients.ToList();
            await RunThreeDownloads(updater, first.AddSeconds(60), TimeSpan.FromMilliseconds(600));
            var secondRound = updater.Clients.Skip(3).ToList();

            Assert.That(race.OwnConnections, Is.True);
            Assert.That(firstRound.Select(c => c.client).Distinct().Count(), Is.EqualTo(3));
            Assert.That(secondRound.Select(c => c.client), Is.EqualTo(firstRound.Select(c => c.client)));
        }

        [Test]
        public async Task SharedConnectionsUseOneClientForAllDownloads()
        {
            var updater = new RacingPageUpdater { Mode = NewUpdater.PageConnections.Shared };

            var race = await RunThreeDownloads(updater, RecentStamp(), TimeSpan.FromMilliseconds(600));

            Assert.That(race.OwnConnections, Is.False);
            Assert.That(updater.Clients.Count, Is.EqualTo(3));
            Assert.That(updater.Clients.Select(c => c.client).Distinct().Count(), Is.EqualTo(1));
        }

        [Test]
        public async Task SharedClientIsNotAnOwnClient()
        {
            var updater = new RacingPageUpdater { Mode = NewUpdater.PageConnections.Shared };
            await RunThreeDownloads(updater, RecentStamp(), TimeSpan.FromMilliseconds(600));
            var shared = updater.Clients.First().client;
            updater.Mode = NewUpdater.PageConnections.Separate;

            await RunThreeDownloads(updater, RecentStamp().AddSeconds(60), TimeSpan.FromMilliseconds(600));

            Assert.That(updater.Clients.Skip(3).Select(c => c.client), Does.Not.Contain(shared));
        }

        [Test]
        public void ConnectionModeDecidesByMinute()
        {
            var odd = new DateTime(2026, 10, 12, 11, 25, 6, DateTimeKind.Utc);
            var even = new DateTime(2026, 10, 12, 11, 26, 6, DateTimeKind.Utc);

            Assert.That(NewUpdater.UsesOwnConnections(NewUpdater.PageConnections.Separate, even), Is.True);
            Assert.That(NewUpdater.UsesOwnConnections(NewUpdater.PageConnections.Shared, odd), Is.False);
            Assert.That(NewUpdater.UsesOwnConnections(NewUpdater.PageConnections.Alternate, odd), Is.True);
            Assert.That(NewUpdater.UsesOwnConnections(NewUpdater.PageConnections.Alternate, even), Is.False);
        }

        [TestCase("separate", "Separate")]
        [TestCase("SHARED", "Shared")]
        [TestCase("alternate", "Alternate")]
        [TestCase("", "Alternate")]
        [TestCase(null, "Alternate")]
        [TestCase("nonsense", "Alternate")]
        [TestCase("7", "Alternate")]
        public void ConnectionModeIsParsedFromTheEnvironmentValue(string value, string expected)
        {
            Assert.That(NewUpdater.ParseConnections(value), Is.EqualTo(Enum.Parse<NewUpdater.PageConnections>(expected)));
        }

        [Test]
        public async Task RaceReportsWallAndCpuTimeOfTheBodyWindow()
        {
            var updater = new RacingPageUpdater();

            var race = await RunThreeDownloads(updater, RecentStamp(), TimeSpan.FromMilliseconds(100));

            Assert.That(race.BodyWall, Is.GreaterThan(TimeSpan.Zero));
            Assert.That(race.BodyCpu, Is.GreaterThanOrEqualTo(TimeSpan.Zero));
            Assert.That(race.BodyGcPause, Is.GreaterThanOrEqualTo(TimeSpan.Zero));
        }

        [Test]
        public void RaceWithoutHandOverReportsZero()
        {
            var race = new NewUpdater.PageRace();

            Assert.That(race.BodyWall, Is.EqualTo(TimeSpan.Zero));
            Assert.That(race.BodyCpu, Is.EqualTo(TimeSpan.Zero));
            Assert.That(race.BodyGcPause, Is.EqualTo(TimeSpan.Zero));
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
