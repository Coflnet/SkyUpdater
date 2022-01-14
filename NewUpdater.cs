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
        private HttpClient httpClient = new HttpClient();

        public async Task DoUpdates(int index, CancellationToken token)
        {
            var lastUpdate = DateTime.Now - TimeSpan.FromMinutes(2);
            // saving offsets
            var ageLookup = new Dictionary<int, int>();
            while (!token.IsCancellationRequested)
            {
                Console.WriteLine("Starting new updater " + DateTime.Now);
                var updateScopeTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(50));
                try
                {
                    var tracer = OpenTracing.Util.GlobalTracer.Instance;
                    using var span = tracer.BuildSpan("FastUpdate").StartActive();
                    using var p = GetProducer();

                    var tasks = new List<ConfiguredTaskAwaitable>();
                    Console.WriteLine($"starting downloads {DateTime.Now} from {lastUpdate}");
                    for (int i = 0; i < 4; i++)
                    {
                        var page = index + i * 10;
                        tasks.Add(Task.Run(async () =>
                        {
                            try
                            {
                                var waitTime = lastUpdate + TimeSpan.FromSeconds(64) - DateTime.Now;
                                if (waitTime < TimeSpan.FromSeconds(0))
                                    waitTime = TimeSpan.FromSeconds(0);
                                await Task.Delay(waitTime);
                                using var siteSpan = tracer.BuildSpan("FastUpdate").AsChildOf(span.Span).WithTag("page", page).StartActive();
                                var time = await GetAndSavePage(page, p, lastUpdate, siteSpan, updateScopeTokenSource.Token);
                                if (page < 20 && time.Item1 > new DateTime(2022, 1, 1))
                                    lastUpdate = time.Item1;
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
                    Console.WriteLine($"sleeping till {lastUpdate + TimeSpan.FromSeconds(60)} " + time);
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


        private Dictionary<int, DateTimeOffset> updated = new Dictionary<int, DateTimeOffset>();
        private async Task<(DateTime, int)> GetAndSavePage(int pageId, IProducer<string, SaveAuction> p, DateTime lastUpdate, OpenTracing.IScope siteSpan, CancellationToken token)
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

            while (page.LastUpdated <= lastUpdate && !token.IsCancellationRequested)
            {
                var message = new HttpRequestMessage(HttpMethod.Post, "https://api.hypixel.net/skyblock/auctions?page=" + pageId);
                try
                {

                    updated.TryGetValue(pageId, out DateTimeOffset offset);
                    if (offset == default)
                        offset = (DateTimeOffset.UtcNow - TimeSpan.FromSeconds(20));
                    message.Headers.IfModifiedSince = offset + TimeSpan.FromSeconds(10);
                }
                catch (Exception e)
                {
                    dev.Logger.Instance.Error(e, "could not set default headers");
                }
                message.Headers.From = "Coflnet";
                using var s = await httpClient.SendAsync(message, HttpCompletionOption.ResponseHeadersRead, token).ConfigureAwait(false);
                var response = DateTime.Now;

                //using var s = await httpClient.GetAsync("https://api.hypixel.net/skyblock/auctions?page=" + pageId, HttpCompletionOption.ResponseHeadersRead, token).ConfigureAwait(false);
                if (s.StatusCode != System.Net.HttpStatusCode.OK)
                {
                    // this is a very cheap request
                    await Task.Delay(REQUEST_BACKOF_DELAY / 3);
                    tryCount++;
                    continue;
                }
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
                        tryCount++;
                        if (tryCount > 10)
                        {
                            // give up and retry next minute
                            return (lastUpdate, 0);
                        }
                        siteSpan.Span.SetTag("try", tryCount);
                        // wait for the server cache to refresh
                        await Task.Delay(REQUEST_BACKOF_DELAY * tryCount);
                        continue;
                    }
                    reader.Read();
                    if (count == 0)
                    {
                        using var prodSpan = OpenTracing.Util.GlobalTracer.Instance.BuildSpan("First")
                            .WithTag("found", DateTime.Now.ToString())
                            .AsChildOf(siteSpan.Span).StartActive();
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
                var lastModified = s.Headers.Where(h => h.Key.ToLower() == "last-modified").Select(h => h.Value).FirstOrDefault()?.FirstOrDefault();
                if (lastModified == null)
                    lastModified = s.Headers.Where(h => h.Key.ToLower() == "date").Select(h => h.Value).FirstOrDefault()?.FirstOrDefault();
                LogHeaderName(siteSpan, s, "last-modified");
                updated[pageId] = DateTimeOffset.Parse(lastModified);

                Console.WriteLine($"Loaded page: {pageId} found {count} ({uuid}) on {DateTime.Now} got: {response} took {tryCount} tries");
            }
            return (page.LastUpdated, age);
        }

        private static void LogHeaderName(OpenTracing.IScope siteSpan, HttpResponseMessage s, string headerName)
        {
            siteSpan.Span.Log($"{headerName}: " + s.Headers.Where(h => h.Key.ToLower() == headerName).Select(h => h.Value).FirstOrDefault()?.FirstOrDefault());
        }
    }
}
