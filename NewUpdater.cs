using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Coflnet.Sky.Updater.Models;
using Confluent.Kafka;
using hypixel;
using Newtonsoft.Json;

namespace Coflnet.Sky.Updater
{
    public class NewUpdater
    {
        private const int REQUEST_BACKOF_DELAY = 180;
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
                    var waitTime = lastUpdate + TimeSpan.FromSeconds(69.9) - DateTime.Now;
                    if (waitTime < TimeSpan.FromSeconds(0))
                        waitTime = TimeSpan.FromSeconds(0);
                    await Task.Delay(waitTime);
                    Console.WriteLine($"starting downloads {DateTime.Now} waited {waitTime}");
                    for (int i = 0; i < 9; i++)
                    {
                        var page = index + i * 10;
                        tasks.Add(Task.Run(async () =>
                        {
                            try
                            {
                                using var siteSpan = tracer.BuildSpan("FastUpdate").AsChildOf(span.Span).WithTag("page", page).StartActive();
                                var time = await GetAndSavePage(page, p, lastUpdate, siteSpan);
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
                    /*
                    var client = new HttpClient();
                    var response = await client.GetAsync("https://api.hypixel.net/skyblock/auctions?page=" + index);
                    Console.WriteLine($"Age: {response.Headers.Where(a=>a.Key == "Age").Select(a=>a.Value).FirstOrDefault()?.FirstOrDefault()}");
                    response = await client.GetAsync("https://api.hypixel.net/skyblock/auctions?page=" + index + new Random().Next(1, 9));
                    Console.WriteLine($"Age: {response.Headers.Where(a=>a.Key == "Age").Select(a=>a.Value).FirstOrDefault()?.FirstOrDefault()}");
                   */

                    Updater.LastPull = lastUpdate;
                    try
                    {
                        BinUpdater.GrabAuctions(new Hypixel.NET.HypixelApi(null, 0));
                    }
                    catch (Exception e)
                    {
                        dev.Logger.Instance.Error(e, "updating sells ");
                    }
                    var time = lastUpdate + TimeSpan.FromSeconds(65) - DateTime.Now;
                    Console.WriteLine($"sleeping till {lastUpdate + TimeSpan.FromSeconds(65)} " + time);
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

        private async Task<DateTime> GetAndSavePage(int pageId, IProducer<string, SaveAuction> p, DateTime lastUpdate, OpenTracing.IScope siteSpan)
        {
            var httpClient = new HttpClient();
            var time = lastUpdate.ToUnix() * 1000;
            var page = new AuctionPage();
            var count = 0;
            while (page.LastUpdated <= lastUpdate)
            {
                var tokenSource = new CancellationTokenSource();
                using var s = await httpClient.GetStreamAsync("https://api.hypixel.net/skyblock/auctions?page=" + pageId, tokenSource.Token).ConfigureAwait(false);
                //page._lastUpdated = root.GetProperty("lastUpdated").GetInt64();

                var serializer = new Newtonsoft.Json.JsonSerializer();
                using (StreamReader sr = new StreamReader(s))
                using (JsonReader reader = new JsonTextReader(sr))
                {
                    for (int i = 0; i < 11; i++)
                    {
                        reader.Read();
                    }
                    page._lastUpdated = (long)reader.Value;
                    if (page.LastUpdated <= lastUpdate)
                    {
                        tokenSource.Cancel();
                        // wait for the server cache to refresh
                        await Task.Delay(REQUEST_BACKOF_DELAY);
                        continue;
                    }
                    reader.Read();
                    if (count == 0)
                    {
                        using var prodSpan = OpenTracing.Util.GlobalTracer.Instance.BuildSpan("First").AsChildOf(siteSpan.Span).StartActive();
                    }
                    await foreach (var auction in reader.SelectTokensWithRegex<Auction>(new System.Text.RegularExpressions.Regex(@"^auctions\[\d+\]$")))
                    {

                        if (auction.Start < lastUpdate)
                            continue;
                        if (count == 0)
                        {
                            using var prodSpan = OpenTracing.Util.GlobalTracer.Instance.BuildSpan("Prod").AsChildOf(siteSpan.Span).StartActive();
                        }
                        Updater.ProduceIntoTopic(Updater.NewAuctionsTopic, p, Updater.ConvertAuction(auction, page.LastUpdated), siteSpan.Span);

                        count++;
                    }
                }

                Console.WriteLine($"Loaded page: {pageId} found {count} on {DateTime.Now} update: {page.LastUpdated}");
            }
            return page.LastUpdated;
        }
    }
}
