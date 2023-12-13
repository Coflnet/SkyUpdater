using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Coflnet.Sky.Updater.Models;
using Confluent.Kafka;
using dev;
using Coflnet.Sky.Core;
using Hypixel.NET;
using RestSharp;
using System.Diagnostics;
using Coflnet.Kafka;

namespace Coflnet.Sky.Updater
{
    /// <summary>
    /// Main implementation for downloading, parsing and forwarding auctions
    /// </summary>
    public class Updater
    {
        private const string LAST_UPDATE_KEY = "lastUpdate";
        private const int REQUEST_BACKOF_DELAY = 200;
        protected static int MillisecondsDelay = Int32.Parse(SimplerConfig.Config.Instance["SLOWDOWN_MS"] ?? "0");
        private string apiKey;
        private HypixelApi apiClient;
        private bool abort;
        private static bool minimumOutput;
        IItemSkinHandler skinHandler;

        private ItemDetailsExtractor extractor = new ItemDetailsExtractor();

        private static string MissingAuctionsTopic = SimplerConfig.Config.Instance["TOPICS:MISSING_AUCTION"];
        public static string SoldAuctionsTopic = SimplerConfig.Config.Instance["TOPICS:SOLD_AUCTION"];
        public static readonly string NewAuctionsTopic = SimplerConfig.Config.Instance["TOPICS:NEW_AUCTION"];
        public static string AuctionEndedTopic = SimplerConfig.Config.Instance["TOPICS:AUCTION_ENDED"];
        public static string NewBidsTopic = SimplerConfig.Config.Instance["TOPICS:NEW_BID"];
        private static string AuctionSumary = SimplerConfig.Config.Instance["TOPICS:AH_SUMARY"];
        private static int DropOffset = Int32.Parse(SimplerConfig.Config.Instance["DROP_OFFSET"] ?? "0");

        private static bool doFullUpdate = false;
        Prometheus.Counter auctionUpdateCount = Prometheus.Metrics.CreateCounter("sky_updater_auction_update", "How many auctions were updated");

        static Prometheus.HistogramConfiguration buckets = new Prometheus.HistogramConfiguration()
        {
            Buckets = Prometheus.Histogram.LinearBuckets(start: 9, width: 2, count: 10)
        };
        static Prometheus.Histogram sendingTime = Prometheus.Metrics.CreateHistogram("sky_updater_time_to_send", "The time from api Update to sending. (should be close to 10)",
            buckets);
        static Prometheus.Histogram timeToFind = Prometheus.Metrics.CreateHistogram("sky_updater_time_to_find", "The time from api Update till deserialising. (should be close to 10)",
            buckets);

        /// <summary>
        /// Index of the updater (if there are multiple updating is split amongst them)
        /// </summary>
        public static int updaterIndex;

        public event Action OnNewUpdateStart;
        /// <summary>
        /// Gets invoked when an update is done
        /// </summary>
        public event Action OnNewUpdateEnd;

        public static DateTime LastPull { get; internal set; }
        public static DateTime LastPullComplete { get; internal set; }
        public static int UpdateSize { get; internal set; }

        Prometheus.Counter newAuctions = Prometheus.Metrics.CreateCounter("sky_updater_new_auctions", "Increases every time a new auction is found");

        public static ActivitySource activitySource;
        public KafkaCreator kafkaCreator;

        /// <summary>
        /// Limited task factory
        /// </summary>
        TaskFactory taskFactory;

        public Updater(string apiKey, IItemSkinHandler skinHandler, ActivitySource pactivitySource, KafkaCreator kafkaCreator)
        {
            this.apiKey = apiKey;
            this.apiClient = new Hypixel.NET.HypixelApi(apiKey, 1);

            var scheduler = new LimitedConcurrencyLevelTaskScheduler(2);
            taskFactory = new TaskFactory(scheduler);
            this.skinHandler = skinHandler;
            activitySource = pactivitySource;
            this.kafkaCreator = kafkaCreator;
        }

        /// <summary>
        /// Downloads all auctions and save the ones that changed since the last update
        /// </summary>
        public async Task<DateTime> Update(bool updateAll = false, CancellationToken token = default)
        {
            doFullUpdate = updateAll;
            if (!minimumOutput)
                Console.WriteLine($"Usage bevore update {System.GC.GetTotalMemory(false)}");
            var updateStartTime = DateTime.UtcNow.ToLocalTime();
            if(updateAll)
                lastUpdateDone = default(DateTime);

            try
            {
                if (lastUpdateDone == default(DateTime))
                    lastUpdateDone = new DateTime(2021, 1, 9, 20, 0, 0);
                lastUpdateDone = await RunUpdate(lastUpdateDone, token);
            }
            catch (Exception e)
            {
                Logger.Instance.Error($"Updating stopped because of {e.Message} {e.StackTrace}  {e.InnerException?.Message} {e.InnerException?.StackTrace}");
                throw e;
            }

            return lastUpdateDone;
        }

        public static DateTime lastUpdateDone = default(DateTime);

        async Task<DateTime> RunUpdate(DateTime updateStartTime, CancellationToken token)
        {
            using var updateScope = activitySource?.CreateActivity("RunUpdate", ActivityKind.Server)?.Start();
            int max = 1;
            var lastUpdate = lastUpdateDone;

            TimeSpan timeEst = new TimeSpan(0, 1, 1);

            // add extra miniute to start to catch lost auctions
            lastUpdate = updateStartTime - new TimeSpan(0, 1, 0);
            DateTime lastHypixelCache = lastUpdate;

            var tasks = new List<ConfiguredTaskAwaitable>();
            int sum = 0;
            int doneCont = 0;
            object sumloc = new object();
            var page = updaterIndex * 10;

            var firstPage = await LoadPage(page, lastUpdate).ConfigureAwait(false);
            Console.WriteLine($"Updating Data {DateTime.Now} " + firstPage.WasSuccessful);

            max = (int)firstPage.TotalPages;
            while (firstPage.LastUpdated == updateStartTime)
            {
                // wait for the server cache to refresh
                await Task.Delay(REQUEST_BACKOF_DELAY);
                firstPage = await LoadPage(page, lastUpdate).ConfigureAwait(false);
                LastPullComplete = DateTime.Now;
            }
            OnNewUpdateStart?.Invoke();

            var binupdate = BinUpdater.GrabAuctions(apiClient).ConfigureAwait(false);

            var cancelToken = new CancellationTokenSource(TimeSpan.FromMinutes(2)).Token;
            cancelToken = CancellationTokenSource.CreateLinkedTokenSource(cancelToken, token).Token;

            var activeUuids = new ConcurrentDictionary<string, bool>();
            Console.WriteLine("loading total pages " + max);
            var sumary = new AhStateSumary();

            using (var p = GetP())
            {
                for (int loopIndexNotUse = 0; loopIndexNotUse < max; loopIndexNotUse++)
                {
                    var index = loopIndexNotUse;
                    await Task.Delay(MillisecondsDelay);
                    tasks.Add(taskFactory.StartNew(async () =>
                    {
                        var tracer = activitySource;
                        using var scope = tracer.StartActivity("LoadPage")?.SetTag("page", index).Start();
                        try
                        {
                            var page = index;

                            if (updaterIndex == 1)
                                page = max - index - 1;
                            if (updaterIndex == 2)
                                page = (index + 40) % max;

                            AuctionPage res;
                            using (var libLoadScope = tracer.StartActivity("LoadPage")?.AddTag("page", index).Start())
                            {
                                res = index != 0 ? await LoadPage(page, lastUpdate).ConfigureAwait(false) : firstPage;
                            }
                            while (res == null)
                            {
                                // tripple the backoff because these will be more
                                await Task.Delay(REQUEST_BACKOF_DELAY * 3);
                                using (var libLoadScope = tracer.StartActivity("LoadPage")?.AddTag("page", index).Start())
                                {
                                    res = await LoadPage(page, lastUpdate).ConfigureAwait(false);
                                }
                            }
                            if (res == null)
                                return;

                            max = (int)res.TotalPages;

                            if (index == 0)
                            {
                                lastHypixelCache = res.LastUpdated;
                                LastPull = res.LastUpdated;
                                // correct update time
                                Console.WriteLine($"Updating difference {lastUpdate} {res.LastUpdated}\n");
                            }

                            var val = await Save(res, lastUpdate, sumary, p, scope?.Context ?? default);
                            scope?.SetTag("lastUpdated", res.LastUpdated.ToString());
                            if (res.LastUpdated == updateStartTime)
                                scope?.SetTag("notUpdated", true);
                            lock (sumloc)
                            {
                                sum += val;
                                // process done
                                doneCont++;
                            }
                            PrintUpdateEstimate(index, doneCont, sum, updateStartTime, max);
                        }
                        catch (Exception e)
                        {
                            scope?.SetTag("error", true);
                            try // again
                            {
                                var res = await LoadPage(page, lastUpdate).ConfigureAwait(false);
                                var val = await Save(res, lastUpdate, sumary, p, scope?.Context ?? default);
                            }
                            catch (System.Exception)
                            {
                                Logger.Instance.Error($"Single page ({index}) could not be loaded twice because of {e.Message} {e.StackTrace} {e.InnerException?.Message}");
                            }
                        }

                    }, cancelToken).Unwrap().ConfigureAwait(false));
                    PrintUpdateEstimate(index, doneCont, sum, updateStartTime, max);

                    // try to stay under 600MB
                    if (System.GC.GetTotalMemory(false) > 500000000)
                    {
                        Console.Write("\t mem: " + System.GC.GetTotalMemory(false));
                        System.GC.Collect();
                    }
                    //await Task.Delay(100);
                }

                foreach (var item in tasks)
                    try
                    {
                        await item;
                    }
                    catch (System.Exception e)
                    {
                        Logger.Instance.Error(e, $"failed to load page");
                    }

                p.Flush(TimeSpan.FromSeconds(10));
            }

            AddSoldAuctions(await binupdate, null);

            Console.WriteLine($"Updated {sum} auctions {doneCont} pages");
            UpdateSize = sum;
            ProduceSummary(sumary);

            doFullUpdate = false;
            OnNewUpdateEnd?.Invoke();

            return lastHypixelCache;
        }

        private void ProduceSummary(AhStateSumary sumary)
        {
            if (updaterIndex <= 1)
                using (var p = GetSumaryProducer())
                {
                    Console.WriteLine($"delivering sumary, size: {MessagePack.MessagePackSerializer.Serialize(sumary).Length}  {sumary.ActiveAuctions.Count}");
                    sumary.Time = DateTime.Now;
                    var p1 = sumary.ActiveAuctions.Take(sumary.ActiveAuctions.Count / 2);
                    var p2 = sumary.ActiveAuctions.Skip(p1.Count());
                    var s1 = CreateSumaryPart(sumary, p1, 1, 2);
                    var s2 = CreateSumaryPart(sumary, p2, 2, 2);
                    Console.WriteLine("actual: " + MessagePack.MessagePackSerializer.Serialize(s1).Length);
                    Console.WriteLine("actual: " + MessagePack.MessagePackSerializer.Serialize(s2).Length);
                    ProduceSumary(s1, p);
                    ProduceSumary(s2, p);
                    p.Flush(TimeSpan.FromSeconds(120));
                }
            else
                Console.WriteLine("skiping sumary");
        }

        private static AhStateSumary CreateSumaryPart(AhStateSumary sumary, IEnumerable<KeyValuePair<long, long>> p1, int part, int partCount)
        {
            return new AhStateSumary() { ActiveAuctions = new ConcurrentDictionary<long, long>(p1), ItemCount = sumary.ItemCount, Time = sumary.Time, Part = part, PartCount = partCount };
        }

        protected virtual IProducer<string, AhStateSumary> GetSumaryProducer()
        {
            return kafkaCreator.BuildProducer<string, AhStateSumary>();
        }

        protected virtual IProducer<string, SaveAuction> GetP()
        {
            return kafkaCreator.BuildProducer<string, SaveAuction>();
        }

        private static void ProduceSumary(AhStateSumary sumary, IProducer<string, AhStateSumary> p)
        {
            p.Produce(AuctionSumary, new Message<string, AhStateSumary> { Value = sumary, Key = sumary.Time.ToString() + sumary.Part }, r =>
            {
                if (r.Error.IsError || r.TopicPartitionOffset.Offset % 100 == 10)
                    Console.WriteLine(!r.Error.IsError ?
                        $"Delivered {r.Topic} {r.Offset} " :
                        $"\nDelivery Error {r.Topic}: {r.Error.Reason}");
            });
        }

        public static bool ShouldPageBeDropped(int page)
        {
            return (page + DropOffset) % 60 == DateTime.Now.Minute;
        }

        private static async Task<AuctionPage> LoadPage(int page, DateTime latUpdate)
        {
            var client = new RestClient("https://api.hypixel.net/v2/skyblock");
            var request = new RestRequest($"auctions?page={page}", Method.Get);
            request.AddHeader("If-Modified-Since", FormatTime(latUpdate));
            //Get the response and Deserialize

            var response = await client.ExecuteAsync(request).ConfigureAwait(false);
            if (response.StatusCode == System.Net.HttpStatusCode.NotModified)
            {
                Console.Write(" not modified " + page);
                return null;
            }
            if (response.Content.Length == 0)
            {
                Console.WriteLine(" | response empty " + page);
                return null;
            }
            try
            {
                return JsonSerializer.Deserialize<AuctionPage>(response.Content);
            }
            catch (Exception e)
            {
                Console.WriteLine("could not parse " + response.Content);
                Console.WriteLine("status " + response.StatusCode);
                throw new Exception("failed to deserialize", e);
            }
        }

        public static string FormatTime(DateTime time)
        {
            return new DateTimeOffset(time.ToUniversalTime()).ToString("ddd, dd MMM yyyy HH:mm:ss 'GMT'", CultureInfo.InvariantCulture);
        }


        internal void UpdateForEver()
        {
            CancellationTokenSource source = new CancellationTokenSource();
            // Fail save
            Task.Run(async () =>
            {
                while (true)
                {
                    await Task.Delay(TimeSpan.FromMinutes(5));
                    if (lastUpdateDone > DateTime.Now.Subtract(TimeSpan.FromMinutes(6)))
                        continue;
                    dev.Logger.Instance.Error("Restarting updater");
                    source.Cancel();
                    source = new CancellationTokenSource();
                    StartNewUpdater(source.Token);
                }
            }).ConfigureAwait(false);
            StartNewUpdater(source.Token);
        }

        private void StartNewUpdater(CancellationToken token)
        {
            Task.Run(async () =>
            {
                minimumOutput = true;
                var updaterStart = DateTime.Now.RoundDown(TimeSpan.FromMinutes(1));
                if (updaterIndex >= 2)
                {
                    await new NewUpdater(activitySource, kafkaCreator).DoUpdates(updaterIndex - 2, token);
                    return;
                }
                Console.WriteLine("Starting updater with index " + updaterIndex);
                while (!token.IsCancellationRequested)
                {
                    try
                    {
                        var start = DateTime.Now;
                        // do a full update 6 min after start
                        var shouldDoFullUpdate = DateTime.Now.Subtract(TimeSpan.FromMinutes(6)).RoundDown(TimeSpan.FromMinutes(1)) == updaterStart;
                        var lastCache = await Update(shouldDoFullUpdate, token);
                        if (abort || token.IsCancellationRequested)
                        {
                            Console.WriteLine("Stopped updater");
                            break;
                        }
                        Console.WriteLine($"--> started updating {start} cache says {lastCache} now its {DateTime.Now}");
                        await WaitForServerCacheRefresh(lastCache);
                    }
                    catch (Exception e)
                    {
                        Logger.Instance.Error("Updater encountered an outside error: " + e.Message);
                        await Task.Delay(15000);
                    }
                }
            }, token).ConfigureAwait(false);
        }

        private static async Task WaitForServerCacheRefresh(DateTime hypixelCacheTime)
        {
            // cache refreshes every 60 seconds, delayed by 10
            var timeToSleep = hypixelCacheTime.Add(TimeSpan.FromSeconds(69)) - DateTime.Now;
            if (timeToSleep.Seconds > 0)
                await Task.Delay(timeToSleep);
        }

        static void PrintUpdateEstimate(long i, long doneCont, long sum, DateTime updateStartTime, long max)
        {
            var index = sum;
            // max is doubled since it is counted twice (download and done)
            var updateEstimation = index * max * 2 / (i + 1 + doneCont) + 1;
            var ticksPassed = (DateTime.Now.ToLocalTime().Ticks - updateStartTime.Ticks);
            var timeEst = new TimeSpan(ticksPassed / (index + 1) * updateEstimation - ticksPassed);
            if (!minimumOutput)
                Console.Write($"\r Loading: ({i}/{max}) Done With: {doneCont} Total:{sum} {timeEst:mm\\:ss}");
        }

        protected virtual async Task<int> Save(AuctionPage res, DateTime lastUpdate, AhStateSumary sumary, IProducer<string, SaveAuction> prod, ActivityContext pageSpanContext)
        {
            List<SaveAuction> processed = new List<SaveAuction>();
            using (var span = activitySource.CreateActivity("parsePage", ActivityKind.Server, pageSpanContext)?.Start())
            {
                processed = res.Auctions.Where(item =>
                    {
                        // nothing changed if the last bid is older than the last update
                        return !(item.Bids.Count > 0 && item.Bids[item.Bids.Count - 1].Timestamp < lastUpdate ||
                            item.Bids.Count == 0 && item.Start < lastUpdate) || doFullUpdate;
                    })
                    .Select(a =>
                    {
                        var auction = ConvertAuction(a, res.LastUpdated);
                        skinHandler.StoreIfNeeded(auction, a);
                        if (auction.Start > lastUpdate)
                            ProduceIntoTopic(new SaveAuction[] { auction }, NewAuctionsTopic, prod, pageSpanContext);
                        return auction;
                    }).ToList();
            }
            // prioritise the flipper
            var started = processed.Where(a => a.Start > lastUpdate).ToList();
            var min = DateTime.Now - TimeSpan.FromMinutes(15);
            //AddToFlipperCheckQueue(started.Where(a => a.Start > min));
            newAuctions.Inc(started.Count());
            ProduceIntoTopic(processed.Where(item => item.Bids.Count > 0 && item.Bids.Max(b => b.Timestamp) > lastUpdate), NewBidsTopic, prod, pageSpanContext);

            var ended = res.Auctions.Where(a => a.End < DateTime.Now).Select(ConvertAuction);
            ProduceIntoTopic(ended, AuctionEndedTopic, prod, pageSpanContext);

            var count = await UpdateSumary(res, sumary);
            return count;
        }

        private async Task<int> UpdateSumary(AuctionPage res, AhStateSumary sumary)
        {
            var count = 0;
            if (updaterIndex == 0 || updaterIndex == 1)
            {
                await Task.Delay(6000);
                foreach (var a in res.Auctions)
                {
                    count++;
                    var auction = ConvertAuction(a);
                    sumary.ActiveAuctions[auction.UId] = auction.End.Ticks;
                    sumary.ItemCount.AddOrUpdate(auction.Tag, 1, (tag, count) => ++count);
                }
                auctionUpdateCount.Inc(count);
            }
            else
                auctionUpdateCount.Inc(1);

            return count;
        }

        public static SaveAuction ConvertAuction(Auction auction)
        {
            return ConvertAuction(auction, DateTime.UtcNow);
        }

        public static SaveAuction ConvertAuction(Auction auction, DateTime apiUpdate)
        {
            var a = new SaveAuction()
            {
                ClaimedBids = auction.ClaimedBidders?.Select(s => new UuId((string)s))?.ToList(),
                Claimed = auction.Claimed,
                //ItemBytes = auction.ItemBytes;
                StartingBid = auction.StartingBid,

                // make sure that the lenght is shorter than max
                ItemName = auction.ItemName,
                End = auction.End,
                Start = auction.Start,
                Coop = auction.Coop,

                ProfileId = auction.ProfileId == auction.Auctioneer ? null : auction.ProfileId,
                AuctioneerId = auction.Auctioneer,
                Uuid = auction.Uuid,
                HighestBidAmount = auction.HighestBidAmount,
                Bids = new List<SaveBids>(),
                Bin = auction.BuyItNow, // missing from nuget package
                UId = AuctionService.Instance.GetId(auction.Uuid),
                Context = new Dictionary<string, string>() { { "upT", apiUpdate.ToString() }, { "fT", (DateTime.Now - apiUpdate).ToString() }, { "lore", auction.ItemLore } }
            };

            if (auction.Bids != null)
                foreach (var bid in auction?.Bids)
                {
                    a.Bids.Add(new SaveBids()
                    {
                        AuctionId = bid.AuctionId.Substring(0, 5),
                        Bidder = bid.Bidder,
                        ProfileId = bid.ProfileId == bid.Bidder ? null : bid.ProfileId,
                        Amount = bid.Amount,
                        Timestamp = bid.Timestamp
                    });
                }
            NBT.FillDetails(a, auction.ItemBytes);
            if (Enum.TryParse(auction.Tier, true, out Tier tier))
                a.Tier = tier;
            else
                a.OldTier = auction.Tier;
            if (Enum.TryParse(auction.Category, true, out Category category))
                a.Category = category;
            else
                a.OldCategory = auction.Category;

            if (apiUpdate != default)
                a.FindTime = apiUpdate;
            return a;
        }

        private class Serializer : ISerializer<SaveAuction>
        {
            public static Serializer Instance = new Serializer();
            public byte[] Serialize(SaveAuction data, SerializationContext context)
            {
                return MessagePack.MessagePackSerializer.Serialize(data);
            }
        }

        public virtual void AddSoldAuctions(IEnumerable<SaveAuction> auctionsToAdd, Activity span)
        {
            ProduceIntoTopic(auctionsToAdd, SoldAuctionsTopic);
        }

        private void ProduceIntoTopic(IEnumerable<SaveAuction> auctionsToAdd, string targetTopic, ActivityContext pageSpanContext = default)
        {
            using var p = kafkaCreator.BuildProducer<string, SaveAuction>();
            ProduceIntoTopic(auctionsToAdd, targetTopic, p, pageSpanContext);

            // wait for up to 10 seconds for any inflight messages to be delivered.
            var lost = p.Flush(TimeSpan.FromSeconds(10));
            if (lost > 0)
                dev.Logger.Instance.Error($"Lost {lost} messages on {targetTopic}");
        }

        private static void ProduceIntoTopic(
            IEnumerable<SaveAuction> auctionsToAdd,
            string targetTopic,
            Confluent.Kafka.IProducer<string, SaveAuction> p,
            ActivityContext pageSpanContext = default)
        {
            foreach (var item in auctionsToAdd)
            {
                var span = activitySource.CreateActivity("Produce", ActivityKind.Server)?.AddTag("topic", targetTopic)?.Start();
                item.TraceContext = new Tracing.TextMap();
                ProduceIntoTopic(targetTopic, p, item, span);
            }
        }

        public static void ProduceIntoTopic(string targetTopic, IProducer<string, SaveAuction> p, SaveAuction item, Activity span)
        {
            if (targetTopic == NewAuctionsTopic)
                timeToFind.Observe((DateTime.Now - item.FindTime).TotalSeconds);

            p.Produce(targetTopic, new Message<string, SaveAuction> { Value = item, Key = $"{item.UId.ToString()}{item.Bids.Count}{item.End}" }, r =>
            {
                if (r.Error.IsError || r.TopicPartitionOffset.Offset % 1000 == 10)
                    Console.WriteLine(!r.Error.IsError ?
                        $"Delivered {r.Topic} {r.Offset} " :
                        $"\nDelivery Error {r.Topic}: {r.Error.Reason}");
                if (r.Topic == NewAuctionsTopic)
                    sendingTime.Observe((DateTime.Now - r.Message.Value.FindTime).TotalSeconds);
                span?.Dispose();
            });
        }

        private static int DetermineWorth(int c, SaveAuction auction)
        {
            var price = auction.HighestBidAmount == 0 ? auction.StartingBid : auction.HighestBidAmount;
            if (price > 500_000)
                return c + 1;
            return c - 20;
        }

        internal void Stop()
        {
            abort = true;
        }
    }
}