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
            LingerMs = 5
        };
        private static HttpClient httpClient = new HttpClient();

        public async Task DoUpdates(int index, CancellationToken token)
        {
            var lastUpdate = DateTime.Now - TimeSpan.FromMinutes(2);
            // saving offsets
            var ageLookup = new Dictionary<int, int>();
            while (!token.IsCancellationRequested)
            {
                Console.WriteLine("Starting new updater " + DateTime.Now);
                try
                {
                    var tracer = OpenTracing.Util.GlobalTracer.Instance;
                    using var span = tracer.BuildSpan("FastUpdate").StartActive();
                    using var p = GetProducer();

                    var tasks = new List<ConfiguredTaskAwaitable>();
                    Console.WriteLine($"starting downloads {DateTime.Now}");
                    for (int i = 0; i < 9; i++)
                    {
                        var page = index + i * 10;
                        tasks.Add(Task.Run(async () =>
                        {
                            try
                            {
                                var secondsAdjust = ageLookup.GetValueOrDefault(page) * 0.5;
                                var waitTime = lastUpdate + TimeSpan.FromSeconds(69.9 - secondsAdjust) - DateTime.Now;
                                if (waitTime < TimeSpan.FromSeconds(0))
                                    waitTime = TimeSpan.FromSeconds(0);
                                await Task.Delay(waitTime);
                                using var siteSpan = tracer.BuildSpan("FastUpdate").AsChildOf(span.Span).WithTag("page", page).StartActive();
                                var time = await GetAndSavePage(page, p, lastUpdate, siteSpan);
                                if (page < 20)
                                    lastUpdate = time.Item1;
                                if (secondsAdjust > 0)
                                    span.Span.Log($"adjusted page {page} by {secondsAdjust}");
                                if (time.Item2 < 3)
                                    ageLookup[page] = time.Item2;

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
                    var time = lastUpdate + TimeSpan.FromSeconds(60) - DateTime.Now;
                    Updater.lastUpdateDone = lastUpdate;
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

        private async Task<(DateTime, int)> GetAndSavePage(int pageId, IProducer<string, SaveAuction> p, DateTime lastUpdate, OpenTracing.IScope siteSpan)
        {
            if (Updater.ShouldPageBeDropped(pageId))
            {
                using var prodSpan = OpenTracing.Util.GlobalTracer.Instance.BuildSpan("DropPage").AsChildOf(siteSpan.Span).StartActive();
                return (lastUpdate, 0);
            }
            var time = lastUpdate.ToUnix() * 1000;
            var page = new AuctionPage();
            var count = 0;
            var tryCount = 0;
            var age = 0;
            string uuid = null;
            while (page.LastUpdated <= lastUpdate)
            {
                var tokenSource = new CancellationTokenSource();
                using var s = await httpClient.GetAsync("https://api.hypixel.net/skyblock/auctions?page=" + pageId, HttpCompletionOption.ResponseHeadersRead, tokenSource.Token).ConfigureAwait(false);
                if (s.StatusCode != System.Net.HttpStatusCode.OK)
                    throw new HttpRequestException();
                //page._lastUpdated = root.GetProperty("lastUpdated").GetInt64();

                var serializer = new Newtonsoft.Json.JsonSerializer();
                using (StreamReader sr = new StreamReader(s.Content.ReadAsStream()))
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
                        tryCount++;
                        // wait for the server cache to refresh
                        await Task.Delay(REQUEST_BACKOF_DELAY * tryCount);
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

                        var prodSpan = OpenTracing.Util.GlobalTracer.Instance.BuildSpan("Prod").AsChildOf(siteSpan.Span).Start();
                        var a = Updater.ConvertAuction(auction, page.LastUpdated);
                        a.Context["upage"] = pageId.ToString();
                        a.Context["utry"] = tryCount.ToString();
                        Updater.ProduceIntoTopic(Updater.NewAuctionsTopic, p, a, prodSpan);
                        uuid = auction.Uuid;

                        count++;
                    }
                }
                LogHeaderName(siteSpan, s, "age");
                LogHeaderName(siteSpan, s, "date");
                LogHeaderName(siteSpan, s, "cf-ray");
                int.TryParse(s.Headers.Where(h => h.Key.ToLower() == "age").Select(h => h.Value).FirstOrDefault()?.FirstOrDefault(), out age);

                Console.WriteLine($"Loaded page: {pageId} found {count} ({uuid}) on {DateTime.Now} update: {page.LastUpdated}");
            }
            return (page.LastUpdated, age);
        }

        private static void LogHeaderName(OpenTracing.IScope siteSpan, HttpResponseMessage s, string headerName)
        {
            siteSpan.Span.Log($"{headerName}: " + s.Headers.Where(h => h.Key.ToLower() == headerName).Select(h => h.Value).FirstOrDefault()?.FirstOrDefault());
        }
    }
}
