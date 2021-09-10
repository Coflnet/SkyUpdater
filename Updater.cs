using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Coflnet;
using Confluent.Kafka;
using dev;
using hypixel;
using Hypixel.NET;
using Hypixel.NET.SkyblockApi;

namespace Coflnet.Sky.Updater
{
    public class Updater
    {
        private const string LAST_UPDATE_KEY = "lastUpdate";
        private static int MillisecondsDelay = Int32.Parse(SimplerConfig.Config.Instance["SLOWDOWN_MS"]);
        private string apiKey;
        private bool abort;
        private static bool minimumOutput;

        private ItemDetailsExtractor extractor = new ItemDetailsExtractor();

        private static string MissingAuctionsTopic = SimplerConfig.Config.Instance["TOPICS:MISSING_AUCTION"];
        private static string SoldAuctionsTopic = SimplerConfig.Config.Instance["TOPICS:SOLD_AUCTION"];
        private static string NewAuctionsTopic = SimplerConfig.Config.Instance["TOPICS:NEW_AUCTION"];
        private static string AuctionEndedTopic = SimplerConfig.Config.Instance["TOPICS:AUCTION_ENDED"];
        private static string NewBidsTopic = SimplerConfig.Config.Instance["TOPICS:NEW_BID"];

        private static bool doFullUpdate = false;
        Prometheus.Counter auctionUpdateCount = Prometheus.Metrics.CreateCounter("auction_update", "How many auctions were updated");

        /// <summary>
        /// Index of the updater (if there are multiple updating is split amongst them)
        /// </summary>
        private static int updaterIndex;

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
        private HypixelApi hypixel;

        public Updater(string apiKey)
        {
            this.apiKey = apiKey;

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
                if (hypixel == null)
                    hypixel = new HypixelApi(apiKey, 50);

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
            var binupdate = Task.Run(()
                => BinUpdater.GrabAuctions(hypixel)).ConfigureAwait(false);

            int max = 1;
            var lastUpdate = lastUpdateDone;

            TimeSpan timeEst = new TimeSpan(0, 1, 1);
            Console.WriteLine($"Updating Data {DateTime.Now}");

            // add extra miniute to start to catch lost auctions
            lastUpdate = updateStartTime - new TimeSpan(0, 1, 0);
            DateTime lastHypixelCache = lastUpdate;

            var tasks = new List<Task>();
            int sum = 0;
            int doneCont = 0;
            object sumloc = new object();
            var firstPage = await hypixel?.GetAuctionPageAsync(0);
            max = (int)firstPage.TotalPages;
            if (firstPage.LastUpdated == updateStartTime)
            {
                // wait for the server cache to refresh
                await Task.Delay(1000);
                return updateStartTime;
            }
            OnNewUpdateStart?.Invoke();

            var cancelToken = new CancellationToken();
            AuctionCount = new ConcurrentDictionary<string, int>();

            var activeUuids = new ConcurrentDictionary<string, bool>();
            using (var p = new ProducerBuilder<string, SaveAuction>(producerConfig).SetValueSerializer(Serializer.Instance).Build())
            {

                for (int i = 0; i < max; i++)
                {
                    var index = i;
                    await Task.Delay(MillisecondsDelay);
                    tasks.Add(taskFactory.StartNew(async () =>
                    {
                        try
                        {
                            var page = index;
                            if (updaterIndex == 1)
                                page = max - i;
                            if (updaterIndex == 2)
                                page = (index + 40) % max;
                            var res = index != 0 ? await hypixel?.GetAuctionPageAsync(page) : firstPage;
                            if (res == null)
                                return;

                            max = (int)res.TotalPages;

                            if (index == 0)
                            {
                                lastHypixelCache = res.LastUpdated;
                                // correct update time
                                Console.WriteLine($"Updating difference {lastUpdate} {res.LastUpdated}\n");
                            }

                            var val = await Save(res, lastUpdate, activeUuids, p);
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
                            try // again
                            {
                                var res = await hypixel?.GetAuctionPageAsync(index);
                                var val = await Save(res, lastUpdate, activeUuids, p);
                            }
                            catch (System.Exception)
                            {
                                Logger.Instance.Error($"Single page ({index}) could not be loaded twice because of {e.Message} {e.StackTrace} {e.InnerException?.Message}");
                            }
                        }

                    }, cancelToken).Unwrap());
                    PrintUpdateEstimate(i, doneCont, sum, updateStartTime, max);

                    // try to stay under 600MB
                    if (System.GC.GetTotalMemory(false) > 500000000)
                    {
                        Console.Write("\t mem: " + System.GC.GetTotalMemory(false));
                        System.GC.Collect();
                    }
                    //await Task.Delay(100);
                }
                p.Flush(TimeSpan.FromSeconds(10));
            }

            await Task.WhenAll(tasks);

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

            doFullUpdate = false;
            OnNewUpdateEnd?.Invoke();

            return lastHypixelCache;
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
                Int32.TryParse(System.Net.Dns.GetHostName().Split('-').Last(), out updaterIndex);
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
            // cache refreshes every 60 seconds, 2 seconds extra to fix timing issues
            var timeToSleep = hypixelCacheTime.Add(TimeSpan.FromSeconds(62)) - DateTime.Now;
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

        Task<int> Save(GetAuctionPage res, DateTime lastUpdate, ConcurrentDictionary<string, bool> activeUuids, IProducer<string, SaveAuction> p)
        {
            int count = 0;

            var processed = res.Auctions.Where(item =>
                {
                    activeUuids[item.Uuid] = true;
                    // nothing changed if the last bid is older than the last update
                    return !(item.Bids.Count > 0 && item.Bids[item.Bids.Count - 1].Timestamp < lastUpdate ||
                        item.Bids.Count == 0 && item.Start < lastUpdate) || doFullUpdate;
                })
                .Select(a =>
                {
                    extractor.AddOrIgnoreDetails(a);
                    count++;
                    var auction = ConvertAuction(a);
                    return auction;
                }).ToList();

            // prioritise the flipper
            var started = processed.Where(a => a.Start > lastUpdate).ToList();
            var min = DateTime.Now - TimeSpan.FromMinutes(15);
            //AddToFlipperCheckQueue(started.Where(a => a.Start > min));
            newAuctions.Inc(started.Count());
            ProduceIntoTopic(started, NewAuctionsTopic, p);
            ProduceIntoTopic(processed.Where(item => item.Bids.Count > 0 && item.Bids[item.Bids.Count - 1].Timestamp > lastUpdate), NewBidsTopic, p);


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
            ProduceIntoTopic(ended, AuctionEndedTopic, p);

            auctionUpdateCount.Inc(count);

            return Task.FromResult(count);
        }

        private static SaveAuction ConvertAuction(Auction auction)
        {
            var a = new SaveAuction()
            {
                ClaimedBids = auction.ClaimedBidders.Select(s => new UuId((string)s)).ToList(),
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

            foreach (var bid in auction.Bids)
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

        private static ProducerConfig producerConfig = new ProducerConfig { BootstrapServers = SimplerConfig.Config.Instance["KAFKA_HOST"] };

        static Action<DeliveryReport<string, SaveAuction>> handler = r =>
            {
                if (r.Error.IsError || r.TopicPartitionOffset.Offset % 1000 == 10)
                    Console.WriteLine(!r.Error.IsError
                        ? $"Delivered {r.Topic} {r.Offset} "
                        : $"\nDelivery Error {r.Topic}: {r.Error.Reason}");
            };

        public static void AddSoldAuctions(IEnumerable<SaveAuction> auctionsToAdd)
        {
            ProduceIntoTopic(auctionsToAdd, SoldAuctionsTopic);
        }

        private static void ProduceIntoTopic(IEnumerable<SaveAuction> auctionsToAdd, string targetTopic)
        {
            using (var p = new ProducerBuilder<string, SaveAuction>(producerConfig).SetValueSerializer(Serializer.Instance).Build())
            {
                ProduceIntoTopic(auctionsToAdd, targetTopic, p);

                // wait for up to 10 seconds for any inflight messages to be delivered.
                p.Flush(TimeSpan.FromSeconds(10));
            }
        }

        private static void ProduceIntoTopic(IEnumerable<SaveAuction> auctionsToAdd, string targetTopic, Confluent.Kafka.IProducer<string, hypixel.SaveAuction> p)
        {
            foreach (var item in auctionsToAdd)
            {
                p.Produce(targetTopic, new Message<string, SaveAuction> { Value = item, Key = $"{item.UId.ToString()}{item.Bids.Count}{item.End}" }, handler);
            }
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
