using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Coflnet.Sky.Updater.Models;
using Confluent.Kafka;
using Coflnet.Sky.Core;
using Newtonsoft.Json;
using RestSharp;
using System.Diagnostics;

namespace Coflnet.Sky.Updater
{
    public class NewUpdater
    {
        private const int REQUEST_BACKOF_DELAY = 60;
        protected virtual string ApiBaseUrl => "https://api.hypixel.net";
        private static ProducerConfig producerConfig = new ProducerConfig
        {
            BootstrapServers = SimplerConfig.Config.Instance["KAFKA_HOST"],
            LingerMs = 10,
            LogConnectionClose = false
        };
        private static HttpClient httpClient = new HttpClient();
        private ActivitySource activitySource;

        public NewUpdater(ActivitySource activitySource)
        {
            this.activitySource = activitySource;
        }

        public async Task DoUpdates(int index, CancellationToken token)
        {
            var lastUpdate = DateTime.Now - TimeSpan.FromMinutes(2);
            while (!token.IsCancellationRequested)
            {
                try
                {
                    using var span = activitySource.CreateActivity("FastUpdate", ActivityKind.Server).Start();
                    using var p = GetProducer();

                    var tasks = new List<ConfiguredTaskAwaitable>();
                    Console.WriteLine($"starting downloads {DateTime.Now}");
                    var page = 0;
                    try
                    {
                        var waitTime = lastUpdate + TimeSpan.FromSeconds(62.8) - DateTime.Now;
                        if (waitTime < TimeSpan.FromSeconds(0))
                            waitTime = TimeSpan.FromSeconds(0);
                        await Task.Delay(waitTime);
                        using var siteSpan = activitySource.CreateActivity("FastUpdate", ActivityKind.Server)?.AddTag("page", page).Start();
                        DateTime time = await DoOneUpdate(lastUpdate, p, page, siteSpan);
                        //var time = await GetAndSavePage(page, p, lastUpdate, siteSpan);
                        if (page < 20)
                            lastUpdate = time;
                    }
                    catch (TaskCanceledException)
                    {
                        Console.Write("canceled " + page);
                    }
                    catch (HttpRequestException e)
                    {
                        Console.WriteLine($"could not get page {page} because {e.Message}\n{e.StackTrace}");
                    }
                    catch (Exception e)
                    {
                        dev.Logger.Instance.Error(e, "update page " + page + e.GetType().Name);
                        await Task.Delay(500);
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

                    Updater.lastUpdateDone = lastUpdate;
                    await Task.Delay(800);
                    var binupdate = await BinUpdater.DownloadSells(ApiBaseUrl).ConfigureAwait(false);
                    ProduceSells(binupdate);
                    var tookTime = lastUpdate + TimeSpan.FromSeconds(60) - DateTime.Now;
                    Console.WriteLine($"sleeping till {lastUpdate + TimeSpan.FromSeconds(60)} " + tookTime);
                    await Task.Delay(tookTime < TimeSpan.Zero ? TimeSpan.FromMilliseconds(500) : tookTime);
                }
                catch (Exception e)
                {
                    dev.Logger.Instance.Error(e, "updating ");
                }
            }
            Console.WriteLine("stopped updating");

        }

        protected virtual async Task<DateTime> DoOneUpdate(DateTime lastUpdate, IProducer<string, SaveAuction> p, int page, Activity siteSpan)
        {
            var pageToken = new CancellationTokenSource(20000);
            var result = await GetAndSavePage(page, p, lastUpdate, siteSpan, pageToken, 0);
            return result.Item1;
        }

        protected virtual void ProduceSells(List<SaveAuction> binupdate)
        {
            Updater.AddSoldAuctions(binupdate, null);
        }

        protected virtual IProducer<string, SaveAuction> GetProducer()
        {
            return new ProducerBuilder<string, SaveAuction>(producerConfig).SetValueSerializer(SerializerFactory.GetSerializer<SaveAuction>()).Build();
        }

        protected async Task<(DateTime, int)> GetAndSavePage(int pageId, IProducer<string, SaveAuction> p, DateTime lastUpdate, Activity siteSpan, CancellationTokenSource pageUpdate = null, int iter = 0)
        {
            await Task.Delay(iter * 200);
            if (pageUpdate.Token.IsCancellationRequested)
                return (lastUpdate, -1);
            var time = lastUpdate.ToUnix() * 1000;
            var page = new AuctionPage();
            var client = GetClient();
            var count = 0;
            var tryCount = 0;
            var age = 0;
            string uuid = null;
            var overallUpdateCancle = new CancellationTokenSource(20000);
            if (pageUpdate != null)
                overallUpdateCancle = pageUpdate;
            var start = DateTime.Now;
            while (page.LastUpdated <= lastUpdate && !overallUpdateCancle.Token.IsCancellationRequested)
            {
                var downloadStart = DateTime.Now;
                var minModTime = DateTimeOffset.Parse("Tue, 11 Jan 2022 09:37:58 GMT") + TimeSpan.FromSeconds(5);
                var message = new HttpRequestMessage(HttpMethod.Post, ApiBaseUrl + "/skyblock/auctions?page=" + pageId);
                message.Headers.IfModifiedSince = minModTime;
                message.Headers.From = "Coflnet";
                CancellationTokenSource tokenSource;
                HttpResponseMessage s;
                try
                {
                    // create new token with timeout
                    tokenSource = new CancellationTokenSource();
                    overallUpdateCancle.Token.Register(tokenSource.Cancel);
                    s = await NewMethod(tokenSource, message).ConfigureAwait(false);
                    tokenSource.Cancel();
                }
                catch (TaskCanceledException)
                {
                    Console.Write("canceled");
                    return (lastUpdate, 0); // was not fast enough
                }
                var intermediate = DateTime.Now;
                if (s.StatusCode != System.Net.HttpStatusCode.OK || s.Headers.Date == httpClient.DefaultRequestHeaders.IfModifiedSince)
                {
                    if (tryCount++ % 20 == 1)
                        Console.Write(" - not changed post " + pageId);
                    await Task.Delay(10);
                    continue;
                }
                if (s.StatusCode != System.Net.HttpStatusCode.OK)
                {
                    Console.WriteLine(s.StatusCode);
                    Console.WriteLine(s.Content);
                    throw new HttpRequestException();
                }

                if (overallUpdateCancle.IsCancellationRequested)
                {
                    pageUpdate.Cancel();
                    return (lastUpdate, 0);
                }
                //page._lastUpdated = root.GetProperty("lastUpdated").GetInt64();

                var serializer = new Newtonsoft.Json.JsonSerializer();
                using (StreamReader sr = new StreamReader(await s.Content.ReadAsStreamAsync().ConfigureAwait(false)))
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
                        siteSpan?.SetTag("try", tryCount);
                        // wait for the server cache to refresh
                        await Task.Delay(REQUEST_BACKOF_DELAY * tryCount);
                        continue;
                    }
                    if (overallUpdateCancle.IsCancellationRequested)
                    {
                        pageUpdate.Cancel();
                        return (lastUpdate, 0);
                    }
                    overallUpdateCancle.Cancel();
                    reader.Read();
                    await foreach (var auction in reader.SelectTokensWithRegex<Auction>(new System.Text.RegularExpressions.Regex(@"^auctions\[\d+\]$")).ConfigureAwait(false))
                    {
                        if (auction.Start < lastUpdate)
                            continue;
                        using var prodSpan = activitySource.CreateActivity("Prod", ActivityKind.Server).Start();
                        FoundNew(pageId, p, page, tryCount, auction, prodSpan);
                        if (count == 0)
                        {
                            var updatedAt = s.Headers.Where(h => h.Key.ToLower() == "date").Select(h => h.Value).FirstOrDefault()?.FirstOrDefault();
                            var tage = s.Headers.Where(h => h.Key.ToLower() == "age").Select(h => h.Value).FirstOrDefault()?.FirstOrDefault();
                            Console.WriteLine($"\nFound first new {auction.Uuid} {auction.Start} {pageId}\t tries:{tryCount}\t i: {iter}");

                            // last "auction found" start
                            Updater.LastPullComplete = intermediate;
                            Updater.LastPull = (page._lastUpdated / 1000).ThisIsNowATimeStamp();
                            Console.WriteLine($"dls:{downloadStart.Second}.{downloadStart.Millisecond} \tparse:{intermediate.Second}.{intermediate.Millisecond} \tnow: {DateTime.Now.Second}.{DateTime.Now.Millisecond} ({DateTime.Now - page.LastUpdated})  \n{updatedAt} {tage}{(DateTime.Now - start)}");
                        }
                        uuid = auction.Uuid;

                        count++;
                    }
                    Console.WriteLine($"done: {DateTime.Now.Second}.{DateTime.Now.Millisecond.ToString("000")}");
                }
                LogHeaderName(siteSpan, s, "age");
                LogHeaderName(siteSpan, s, "date");
                LogHeaderName(siteSpan, s, "cf-ray");
                int.TryParse(s.Headers.Where(h => h.Key.ToLower() == "age").Select(h => h.Value).FirstOrDefault()?.FirstOrDefault(), out age);

                Console.WriteLine($"Loaded page: {pageId} found {count} ({uuid}) on {DateTime.Now} {DateTime.Now.Millisecond} update: {page.LastUpdated}");
            }
            return (page.LastUpdated, age);
        }

        protected virtual void FoundNew(int pageId, IProducer<string, SaveAuction> p, AuctionPage page, int tryCount, Auction auction, Activity prodSpan)
        {
            var a = Updater.ConvertAuction(auction, page.LastUpdated);
            a.Context["upage"] = pageId.ToString();
            a.Context["utry"] = tryCount.ToString();
            Updater.ProduceIntoTopic(Updater.NewAuctionsTopic, p, a, prodSpan);
        }

        protected virtual async Task<HttpResponseMessage> NewMethod(CancellationTokenSource tokenSource, HttpRequestMessage message)
        {
            return await httpClient.SendAsync(message, HttpCompletionOption.ResponseHeadersRead, tokenSource.Token).ConfigureAwait(false);
        }


        private RestClient _restClient;
        protected virtual RestClient GetClient()
        {
            if (_restClient == null)
                _restClient = new RestClient(ApiBaseUrl);
            return _restClient;
        }

        private static void LogHeaderName(Activity siteSpan, HttpResponseMessage s, string headerName)
        {
            siteSpan?.AddEvent(new("log", default, new(new Dictionary<string, object>() { { 
                "message", $"{headerName}: " + s.Headers.Where(h => h.Key.ToLower() == headerName).Select(h => h.Value).FirstOrDefault()?.FirstOrDefault()}
                })));
        }
    }
}
