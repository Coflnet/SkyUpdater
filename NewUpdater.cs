using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Coflnet.Sky.Updater.Models;
using Confluent.Kafka;
using hypixel;

namespace Coflnet.Sky.Updater
{
    public class NewUpdater
    {
        private const int REQUEST_BACKOF_DELAY = 80;
        private static ProducerConfig producerConfig = new ProducerConfig
        {
            BootstrapServers = SimplerConfig.Config.Instance["KAFKA_HOST"],
            LingerMs = 0
        };

        public async Task DoUpdates(int index, CancellationToken token)
        {
            var lastUpdate = DateTime.Now - TimeSpan.FromMinutes(2);
            while (!token.IsCancellationRequested)
            {
                Console.WriteLine("Starting new updater " + DateTime.Now);
                var tracer = OpenTracing.Util.GlobalTracer.Instance;
                using var span = tracer.BuildSpan("FastUpdate").StartActive();
                try
                {
                    using var p = GetProducer();

                    var tasks = new List<ConfiguredTaskAwaitable>();
                    var waitTime = lastUpdate + TimeSpan.FromSeconds(70) - DateTime.Now;
                    if (waitTime < TimeSpan.FromSeconds(0))
                        waitTime = TimeSpan.FromSeconds(0);
                    await Task.Delay(waitTime);
                    for (int i = 0; i < 9; i++)
                    {
                        var page = index + i * 10;
                        tasks.Add(Task.Run(async () =>
                        {
                            try
                            {
                                using var siteSpan = tracer.BuildSpan("FastUpdate").AsChildOf(span.Span).WithTag("page", page).StartActive();
                                var time = await GetAndSavePage(page, p, lastUpdate);
                                if (page < 10)
                                    lastUpdate = time;

                            }
                            catch (HttpRequestException e)
                            {
                                Console.WriteLine("could not get page " + page);
                            }
                            catch (Exception e)
                            {
                                dev.Logger.Instance.Error(e, "update page " + page + e.GetType().Name);
                            }
                        }).ConfigureAwait(false));
                    }
                    foreach (var item in tasks)
                    {
                        await item;
                    }
                    // wait for up to 10 seconds for any inflight messages to be delivered.
                    p.Flush(TimeSpan.FromSeconds(10));

                    var client = new HttpClient();
                    var response = await client.GetAsync("https://api.hypixel.net/skyblock/auctions?page=" + index);
                    foreach (var item in response.Headers)
                    {
                        Console.WriteLine(item.Key + " " + item.Value);
                    }
                    response = await client.GetAsync("https://api.hypixel.net/skyblock/auctions?page=" + index + new Random().Next(1, 9));
                    foreach (var item in response.Headers)
                    {
                        Console.WriteLine(item.Key + " " + item.Value);
                    }


                    var time = lastUpdate + TimeSpan.FromSeconds(68) - DateTime.Now;
                    Console.WriteLine($"sleeping till {lastUpdate + TimeSpan.FromSeconds(66)} " + time);
                    await Task.Delay(time < TimeSpan.Zero ? TimeSpan.Zero : time);
                }
                catch (Exception e)
                {
                    dev.Logger.Instance.Error(e, "updating ");
                }
            }

        }

        protected virtual IProducer<string, SaveAuction> GetProducer()
        {
            return new ProducerBuilder<string, SaveAuction>(producerConfig).SetValueSerializer(SerializerFactory.GetSerializer<SaveAuction>()).Build();
        }

        private async Task<DateTime> GetAndSavePage(int pageId, IProducer<string, SaveAuction> p, DateTime lastUpdate)
        {
            var httpClient = new HttpClient();
            var time = lastUpdate.ToUnix() * 1000;
            var page = new AuctionPage();
            var count = 0;
            while (page.LastUpdated <= lastUpdate)
            {
                using var document = await JsonDocument.ParseAsync(await httpClient.GetStreamAsync("https://api.hypixel.net/skyblock/auctions?page=" + pageId)).ConfigureAwait(false);
                var root = document.RootElement;
                page._lastUpdated = root.GetProperty("lastUpdated").GetInt64();
                if (page.LastUpdated <= lastUpdate)
                {
                    // wait for the server cache to refresh
                    await Task.Delay(REQUEST_BACKOF_DELAY);
                    continue;
                }

                foreach (var item in document.RootElement.GetProperty("auctions").EnumerateArray())
                {
                    var start = item.GetProperty("start").GetInt64();
                    if (start > time)
                    {
                        var auction = new Auction()
                        {
                            Uuid = item.GetProperty("uuid").GetString(),
                            ItemName = item.GetProperty("item_name").GetString(),
                            StartingBid = item.GetProperty("starting_bid").GetInt64(),
                            Tier = item.GetProperty("tier").GetString(),
                            Auctioneer = item.GetProperty("auctioneer").GetString(),
                            ItemBytes = item.GetProperty("item_bytes").GetString(),
                            _end = item.GetProperty("end").GetInt64(),
                            _start = start,
                            Category = item.GetProperty("category").GetString(),
                            HighestBidAmount = item.GetProperty("highest_bid_amount").GetInt64(),
                            ItemLore = item.GetProperty("item_lore").GetString()
                        };
                        if (item.TryGetProperty("bin", out JsonElement elem))
                            auction.BuyItNow = true;
                        try
                        {
                            Updater.ProduceIntoTopic(Updater.NewAuctionsTopic, p, Updater.ConvertAuction(auction, page.LastUpdated), null);
                        }
                        catch (Exception e)
                        {
                            dev.Logger.Instance.Error(e, JsonSerializer.Serialize(auction));
                        }
                        count++;
                    }
                }
                Console.WriteLine($"Loaded page: {pageId} found {count} on {DateTime.Now} update: {page.LastUpdated}");
            }
            return page.LastUpdated;
        }
    }
}
