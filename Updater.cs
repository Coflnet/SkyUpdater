using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Coflnet;
using Coflnet.Sky.Updater.Models;
using Confluent.Kafka;
using dev;
using hypixel;
using Hypixel.NET;
using OpenTracing;
using OpenTracing.Propagation;
using OpenTracing.Util;
using RestSharp;

namespace Coflnet.Sky.Updater
{
    public class Updater
    {
        private const string LAST_UPDATE_KEY = "lastUpdate";
        private const int REQUEST_BACKOF_DELAY = 200;
        private static int MillisecondsDelay = Int32.Parse(SimplerConfig.Config.Instance["SLOWDOWN_MS"]);
        private string apiKey;
        private HypixelApi apiClient;
        private bool abort;
        private static bool minimumOutput;

        private ItemDetailsExtractor extractor = new ItemDetailsExtractor();

        private static string MissingAuctionsTopic = SimplerConfig.Config.Instance["TOPICS:MISSING_AUCTION"];
        private static string SoldAuctionsTopic = SimplerConfig.Config.Instance["TOPICS:SOLD_AUCTION"];
        public static readonly string NewAuctionsTopic = SimplerConfig.Config.Instance["TOPICS:NEW_AUCTION"];
        private static string AuctionEndedTopic = SimplerConfig.Config.Instance["TOPICS:AUCTION_ENDED"];
        private static string NewBidsTopic = SimplerConfig.Config.Instance["TOPICS:NEW_BID"];
        private static string AuctionSumary = SimplerConfig.Config.Instance["TOPICS:AH_SUMARY"];

        private static bool doFullUpdate = false;
        Prometheus.Counter auctionUpdateCount = Prometheus.Metrics.CreateCounter("auction_update", "How many auctions were updated");

        static Prometheus.HistogramConfiguration buckets = new Prometheus.HistogramConfiguration()
        {
            Buckets = Prometheus.Histogram.LinearBuckets(start: 0, width: 2, count: 10)
        };
        static Prometheus.Histogram sendingTime = Prometheus.Metrics.CreateHistogram("timeToSending", "The time from api Update to sending. (should be close to 10)",
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
        public static int UpdateSize { get; internal set; }

        Prometheus.Counter newAuctions = Prometheus.Metrics.CreateCounter("new_auction", "Increases every time a new auction is found");

        private static ConcurrentDictionary<string, bool> ActiveAuctions = new ConcurrentDictionary<string, bool>();
        private static ConcurrentDictionary<string, DateTime> MissingSince = new ConcurrentDictionary<string, DateTime>();

        ConcurrentDictionary<string, int> AuctionCount;
        public static ConcurrentDictionary<string, int> LastAuctionCount;

        /// <summary>
        /// Limited task factory
        /// </summary>
        TaskFactory taskFactory;


        public Updater(string apiKey)
        {
            this.apiKey = apiKey;
            this.apiClient = new Hypixel.NET.HypixelApi(apiKey, 1);

            var scheduler = new LimitedConcurrencyLevelTaskScheduler(3);
            taskFactory = new TaskFactory(scheduler);
        }

        /// <summary>
        /// Downloads all auctions and save the ones that changed since the last update
        /// </summary>
        public async Task<DateTime> Update(bool updateAll = false)
        {
            doFullUpdate = updateAll;
            if (!minimumOutput)
                Console.WriteLine($"Usage bevore update {System.GC.GetTotalMemory(false)}");
            var updateStartTime = DateTime.UtcNow.ToLocalTime();

            try
            {
                if (lastUpdateDone == default(DateTime))
                    lastUpdateDone = await CacheService.Instance.GetFromRedis<DateTime>(LAST_UPDATE_KEY);

                if (lastUpdateDone == default(DateTime))
                    lastUpdateDone = new DateTime(2017, 1, 1);
                lastUpdateDone = await RunUpdate(lastUpdateDone);
                FileController.SaveAs(LAST_UPDATE_KEY, lastUpdateDone);
                await CacheService.Instance.SaveInRedis(LAST_UPDATE_KEY, lastUpdateDone);
                FileController.Delete("lastUpdateStart");
            }
            catch (Exception e)
            {
                Logger.Instance.Error($"Updating stopped because of {e.Message} {e.StackTrace}  {e.InnerException?.Message} {e.InnerException?.StackTrace}");
                FileController.Delete("lastUpdateStart");
                throw e;
            }

            ItemDetails.Instance.Save();

            return lastUpdateDone;
        }

        DateTime lastUpdateDone = default(DateTime);

        async Task<DateTime> RunUpdate(DateTime updateStartTime)
        {

            var tracer = GlobalTracer.Instance;
            using var updateScope = tracer.BuildSpan("RunUpdate").StartActive();
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

            var firstPage = await LoadPage(page).ConfigureAwait(false);
            Console.WriteLine($"Updating Data {DateTime.Now} " + firstPage.WasSuccessful);

            max = (int)firstPage.TotalPages;
            while (firstPage.LastUpdated == updateStartTime)
            {
                // wait for the server cache to refresh
                await Task.Delay(REQUEST_BACKOF_DELAY);
                firstPage = await LoadPage(page).ConfigureAwait(false);
            }
            OnNewUpdateStart?.Invoke();

            var binupdate = Task.Run(()
                => BinUpdater.GrabAuctions(apiClient)).ConfigureAwait(false);

            var cancelToken = new CancellationToken();
            AuctionCount = new ConcurrentDictionary<string, int>();

            var activeUuids = new ConcurrentDictionary<string, bool>();
            Console.WriteLine("loading total pages " + max);
            var sumary = new AhStateSumary();

            using (var p = new ProducerBuilder<string, SaveAuction>(producerConfig).SetValueSerializer(Serializer.Instance).Build())
            {
                for (int loopIndexNotUse = 0; loopIndexNotUse < max; loopIndexNotUse++)
                {
                    var index = loopIndexNotUse;
                    await Task.Delay(MillisecondsDelay);
                    tasks.Add(taskFactory.StartNew(async () =>
                    {
                        var tracer = GlobalTracer.Instance;
                        using var scope = tracer.BuildSpan("LoadPage").WithTag("page", index).StartActive();
                        try
                        {
                            var page = index;

                            if (updaterIndex == 1)
                                page = max - index;
                            if (updaterIndex == 2)
                                page = (index + 40) % max;
                            AuctionPage res;
                            using (var libLoadScope = tracer.BuildSpan("LoadPage").WithTag("page", index).StartActive())
                            {
                                res = index != 0 ? await LoadPage(page).ConfigureAwait(false) : firstPage;
                            }
                            while (res == null || res.LastUpdated == updateStartTime)
                            {
                                // tripple the backoff because these will be more
                                await Task.Delay(REQUEST_BACKOF_DELAY * 3);
                                using (var libLoadScope = tracer.BuildSpan("LoadPage").WithTag("page", index).StartActive())
                                {
                                    res = await LoadPage(page).ConfigureAwait(false);
                                }
                            }
                            if (res == null)
                                return;

                            max = (int)res.TotalPages;

                            if (index == 0)
                            {
                                lastHypixelCache = res.LastUpdated;
                                // correct update time
                                Console.WriteLine($"Updating difference {lastUpdate} {res.LastUpdated}\n");
                            }

                            var val = await Save(res, lastUpdate, sumary, p, scope.Span.Context);
                            scope.Span.SetTag("lastUpdated", res.LastUpdated.ToString());
                            if (res.LastUpdated == updateStartTime)
                                scope.Span.SetTag("notUpdated", true);
                            scope.Span.Log($"Loaded {val}");
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
                            scope.Span.SetTag("error", true);
                            try // again
                            {
                                var res = await LoadPage(page).ConfigureAwait(false);
                                var val = await Save(res, lastUpdate, sumary, p, scope.Span.Context);
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
                    await item;



                p.Flush(TimeSpan.FromSeconds(10));
            }

            if (AuctionCount.Count > 2)
                LastAuctionCount = AuctionCount;

            //BinUpdateSold(currentUpdateBins);
            var lastUuids = ActiveAuctions;
            ActiveAuctions = activeUuids;
            var canceledTask = Task.Run(() =>
            {
                RemoveCanceled(lastUuids);
            }).ConfigureAwait(false);

            if (sum > 10)
                LastPull = DateTime.Now;

            Console.WriteLine($"Updated {sum} auctions {doneCont} pages");
            UpdateSize = sum;

            if (updaterIndex == 0)
                using (var p = new ProducerBuilder<string, AhStateSumary>(producerConfig).SetValueSerializer(SerializerFactory.GetSerializer<AhStateSumary>()).Build())
                {
                    Console.WriteLine("delivering sumary");
                    sumary.Time = DateTime.Now;
                    p.Produce(AuctionSumary, new Message<string, AhStateSumary> { Value = sumary, Key = "" }, r =>
                    {
                        if (r.Error.IsError || r.TopicPartitionOffset.Offset % 100 == 10)
                            Console.WriteLine(!r.Error.IsError
                                ? $"Delivered {r.Topic} {r.Offset} "
                                : $"\nDelivery Error {r.Topic}: {r.Error.Reason}");
                    });
                    p.Flush(TimeSpan.FromSeconds(10));
                }


            doFullUpdate = false;
            OnNewUpdateEnd?.Invoke();

            return lastHypixelCache;
        }

        private static async Task<AuctionPage> LoadPage(int page)
        {
            var client = new RestClient("https://api.hypixel.net/skyblock");
            var request = new RestRequest($"auctions?page={page}", Method.GET);
            //Get the response and Deserialize

            var response = await client.ExecuteAsync(request).ConfigureAwait(false);
            return JsonSerializer.Deserialize<AuctionPage>(response.Content);
        }

        /// <summary>
        /// Takes care of removing canceled auctions
        /// Will check 5 updates to make sure there wasn't just a page missing
        /// </summary>
        /// <param name="lastUuids"></param>
        private static void RemoveCanceled(ConcurrentDictionary<string, bool> lastUuids)
        {
            foreach (var item in ActiveAuctions.Keys)
            {
                lastUuids.TryRemove(item, out bool val);
                MissingSince.TryRemove(item, out DateTime value);
            }

            foreach (var item in BinUpdater.SoldLastMin)
            {
                lastUuids.TryRemove(item.Uuid, out bool val);
            }

            foreach (var item in lastUuids)
            {
                MissingSince[item.Key] = DateTime.Now;
                // its less important if items are removed from the flipper than globally
                // the flipper should not display inactive auctions at all
                // TODO update this
                //Flipper.FlipperEngine.Instance.AuctionInactive(item.Key);
            }
            var removed = new HashSet<string>();
            foreach (var item in MissingSince)
            {
                if (item.Value < DateTime.Now - TimeSpan.FromMinutes(5))
                    removed.Add(item.Key);
            }
            ProduceIntoTopic(removed.Select(uuid => new SaveAuction()
            {
                Uuid = uuid,
                UId = AuctionService.Instance.GetId(uuid),
                End = DateTime.Now
            }), MissingAuctionsTopic);
            foreach (var item in removed)
            {
                MissingSince.TryRemove(item, out DateTime since);
            }
            Console.WriteLine($"Canceled last min: {removed.Count} {removed.FirstOrDefault()}");
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
                    await new NewUpdater().DoUpdates(updaterIndex - 2, token);
                    return;
                }
                Console.WriteLine("Starting updater with index " + updaterIndex);
                while (true)
                {
                    try
                    {
                        var start = DateTime.Now;
                        // do a full update 6 min after start
                        var shouldDoFullUpdate = DateTime.Now.Subtract(TimeSpan.FromMinutes(6)).RoundDown(TimeSpan.FromMinutes(1)) == updaterStart;
                        var lastCache = await Update(shouldDoFullUpdate);
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
            // cache refreshes every 60 seconds, delayed by 10, 1 seconds extra to fix timing issues
            var timeToSleep = hypixelCacheTime.Add(TimeSpan.FromSeconds(71)) - DateTime.Now;
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

        // builds the index for all auctions in the last hour

        async Task<int> Save(AuctionPage res, DateTime lastUpdate, AhStateSumary sumary, IProducer<string, SaveAuction> p, ISpanContext pageSpanContext)
        {
            List<SaveAuction> processed = new List<SaveAuction>();
            using (var span = GlobalTracer.Instance.BuildSpan("parsePage").AsChildOf(pageSpanContext).StartActive())
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
                        if (auction.Start > lastUpdate)
                            ProduceIntoTopic(new SaveAuction[] { auction }, NewAuctionsTopic, p, pageSpanContext);
                        return auction;
                    }).ToList();
            }
            // prioritise the flipper
            var started = processed.Where(a => a.Start > lastUpdate).ToList();
            var min = DateTime.Now - TimeSpan.FromMinutes(15);
            //AddToFlipperCheckQueue(started.Where(a => a.Start > min));
            newAuctions.Inc(started.Count());
            ProduceIntoTopic(processed.Where(item => item.Bids.Count > 0 && item.Bids.Max(b => b.Timestamp) > lastUpdate), NewBidsTopic, p, pageSpanContext);

            if (DateTime.Now.Minute % 30 == 7)
                foreach (var a in res.Auctions)
                {
                    var auction = ConvertAuction(a);
                    AuctionCount.AddOrUpdate(auction.Tag, k =>
                    {
                        return DetermineWorth(0, auction);
                    }, (k, c) =>
                    {
                        return DetermineWorth(c, auction);
                    });
                }

            var ended = res.Auctions.Where(a => a.End < DateTime.Now).Select(ConvertAuction);
            ProduceIntoTopic(ended, AuctionEndedTopic, p, pageSpanContext);

            var count = await UpdateSumary(res, sumary);
            return count;
        }

        private async Task<int> UpdateSumary(AuctionPage res, AhStateSumary sumary)
        {
            var count = 0;
            if (updaterIndex == 0 || updaterIndex == 1)
            {
                await Task.Delay(8000);
                foreach (var a in res.Auctions)
                {
                    extractor.AddOrIgnoreDetails(a);
                    count++;
                    var auction = ConvertAuction(a);
                    sumary.ActiveAuctions[auction.UId] = 1;
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
            return ConvertAuction(auction, default);
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

        private static ProducerConfig producerConfig = new ProducerConfig
        {
            BootstrapServers = SimplerConfig.Config.Instance["KAFKA_HOST"],
            LingerMs = 2
        };


        public static void AddSoldAuctions(IEnumerable<SaveAuction> auctionsToAdd, IScope span)
        {
            ProduceIntoTopic(auctionsToAdd, SoldAuctionsTopic, span?.Span?.Context);
        }

        private static void ProduceIntoTopic(IEnumerable<SaveAuction> auctionsToAdd, string targetTopic, ISpanContext pageSpanContext = null)
        {
            using (var p = new ProducerBuilder<string, SaveAuction>(producerConfig).SetValueSerializer(Serializer.Instance).Build())
            {
                ProduceIntoTopic(auctionsToAdd, targetTopic, p, pageSpanContext);

                // wait for up to 10 seconds for any inflight messages to be delivered.
                p.Flush(TimeSpan.FromSeconds(10));
            }
        }

        private static void ProduceIntoTopic(
            IEnumerable<SaveAuction> auctionsToAdd,
            string targetTopic,
            Confluent.Kafka.IProducer<string, hypixel.SaveAuction> p,
            ISpanContext pageSpanContext = null)
        {
            var tracer = OpenTracing.Util.GlobalTracer.Instance;
            foreach (var item in auctionsToAdd)
            {
                var builder = tracer.BuildSpan("Produce").WithTag("topic", targetTopic);
                if (pageSpanContext != null)
                    builder = builder.AsChildOf(pageSpanContext);
                var span = builder.Start();

                item.TraceContext = new Tracing.TextMap();
                tracer.Inject(span.Context, BuiltinFormats.TextMap, item.TraceContext);
                ProduceIntoTopic(targetTopic, p, item, span);
            }
        }

        public static void ProduceIntoTopic(string targetTopic, IProducer<string, SaveAuction> p, SaveAuction item, ISpan span)
        {
            p.Produce(targetTopic, new Message<string, SaveAuction> { Value = item, Key = $"{item.UId.ToString()}{item.Bids.Count}{item.End}" }, r =>
            {
                if (r.Error.IsError || r.TopicPartitionOffset.Offset % 1000 == 10)
                    Console.WriteLine(!r.Error.IsError
                        ? $"Delivered {r.Topic} {r.Offset} "
                        : $"\nDelivery Error {r.Topic}: {r.Error.Reason}");
                if (r.Topic == NewAuctionsTopic)
                    sendingTime.Observe((DateTime.Now - r.Message.Value.FindTime).TotalSeconds);
                span.Finish();
            });
        }

        public static void AddToFlipperCheckQueue(IEnumerable<SaveAuction> auctionsToAdd)
        {
            ProduceIntoTopic(auctionsToAdd, "sky-flipper");
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
