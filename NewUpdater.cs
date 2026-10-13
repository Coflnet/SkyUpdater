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
using dev;
using Prometheus;

namespace Coflnet.Sky.Updater;

public class NewUpdater
{
    private const int REQUEST_BACKOF_DELAY = 10;
    internal const double PollStartSeconds = 63.9;
    /// <summary>
    /// Hypixel lists an auction 20 seconds after it started, so a snapshot also brings auctions that started up to
    /// 20 seconds before the previous stamp. 25 leaves room for jitter.
    /// </summary>
    internal static readonly TimeSpan PublishDelay = TimeSpan.FromSeconds(25);
    protected virtual string ApiBaseUrl => "https://api.hypixel.net";
    private readonly HttpClient httpClient = CreatePageClient();
    /// <summary>
    /// One client (and with it one connection) per download id, kept so the connection stays pooled between rounds
    /// </summary>
    private readonly System.Collections.Concurrent.ConcurrentDictionary<int, HttpClient> ownClients = new();
    // Alternate is the measurement setting: rounds alternate so shared and separate connections can be compared
    // on the same pods, to be fixed to one value once measured
    private static readonly PageConnections configuredConnections = ParseConnections(Environment.GetEnvironmentVariable("PAGE_CONNECTIONS"));
    private protected virtual PageConnections Connections => configuredConnections;
    internal static HttpClient CreatePageClient()
    {
        var client = new HttpClient(new SocketsHttpHandler()
        {
            // gzip only: brotli costs 30-40ms extra ttfb (cloudflare recompresses on the fly)
            AutomaticDecompression = System.Net.DecompressionMethods.GZip,
            EnableMultipleHttp2Connections = true,
            PooledConnectionIdleTimeout = TimeSpan.FromMinutes(5),
            InitialHttp2StreamWindowSize = 4 * 1024 * 1024 // default 64KB throttles the MB-sized page bodies
        })
        {
            DefaultRequestVersion = System.Net.HttpVersion.Version20,
            DefaultVersionPolicy = HttpVersionPolicy.RequestVersionOrLower
        };
        client.DefaultRequestHeaders.ConnectionClose = false;
        return client;
    }

    /// <summary>
    /// Whether the downloads of a round use a connection each or share one
    /// </summary>
    internal enum PageConnections { Separate, Shared, Alternate }

    internal static PageConnections ParseConnections(string value)
    {
        return Enum.TryParse<PageConnections>(value, true, out var mode) && Enum.IsDefined(mode) ? mode : PageConnections.Alternate;
    }

    internal static bool UsesOwnConnections(PageConnections mode, DateTime utcNow)
    {
        return mode switch
        {
            PageConnections.Separate => true,
            PageConnections.Shared => false,
            _ => utcNow.Minute % 2 == 1
        };
    }

    /// <summary>
    /// Starts a round, the connection mode is chosen once for all its downloads
    /// </summary>
    protected PageRace NewRace() => new PageRace { OwnConnections = UsesOwnConnections(Connections, DateTime.UtcNow) };

    /// <summary>
    /// When the page with the snapshot we currently have finished downloading
    /// </summary>
    private DateTime lastReceived;
    private readonly SeenAuctions seenAuctions = new();
    private ActivitySource activitySource;
    private Kafka.KafkaCreator kafkaCreator;
    private readonly Gauge firstByteTime = Metrics.CreateGauge("sky_update_first_byte", "Time till first byte");
    private readonly Gauge firstParsed = Metrics.CreateGauge("sky_update_first_parsed", "Time till first parsed auction");
    private readonly Gauge pageOneDone = Metrics.CreateGauge("sky_update_page_one_done", "Time till first page done");

    public NewUpdater(ActivitySource activitySource, Kafka.KafkaCreator kafkaCreator)
    {
        this.activitySource = activitySource;
        this.kafkaCreator = kafkaCreator;
    }

    public async Task DoUpdates(int index, CancellationToken token)
    {
        var lastUpdate = DateTime.Now - TimeSpan.FromMinutes(2);
        while (!token.IsCancellationRequested)
        {
            try
            {
                using var span = activitySource.CreateActivity("FastUpdate", ActivityKind.Server)?.Start();
                using var p = GetProducer();

                var tasks = new List<ConfiguredTaskAwaitable>();
                Console.WriteLine($"starting downloads {DateTime.Now}");
                var page = 0;
                try
                {
                    var waitTime = lastUpdate + TimeSpan.FromSeconds(PollStartSeconds) - DateTime.Now;
                    if (waitTime < TimeSpan.FromSeconds(0))
                        waitTime = TimeSpan.FromSeconds(0);
                    await Task.Delay(waitTime);
                    using var siteSpan = activitySource.CreateActivity("PageUpdate", ActivityKind.Server)?.AddTag("page", page)?.Start();
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
                await UpdateSells(BinUpdater.DownloadSells(ApiBaseUrl), lastUpdate + TimeSpan.FromSeconds(60) - DateTime.Now).ConfigureAwait(false);
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

    internal async Task<bool> UpdateSells(Task<List<SaveAuction>> download, TimeSpan timeLeft)
    {
        if (timeLeft < TimeSpan.Zero)
            timeLeft = TimeSpan.Zero;
        if (await Task.WhenAny(download, Task.Delay(timeLeft)).ConfigureAwait(false) != download)
        {
            Console.WriteLine("sold auctions took too long, continuing with the next update");
            // observe the exception of the abandoned download
            _ = download.ContinueWith(t => _ = t.Exception, TaskContinuationOptions.OnlyOnFaulted);
            return false;
        }
        ProduceSells(await download.ConfigureAwait(false));
        return true;
    }

    protected virtual void ProduceSells(List<SaveAuction> binupdate)
    {
        using var p = GetProducer();
        foreach (var item in binupdate)
        {
            Updater.ProduceIntoTopic(Updater.SoldAuctionsTopic, p, item, null);
        }
        p.Flush(TimeSpan.FromSeconds(30));
    }

    protected virtual IProducer<string, SaveAuction> GetProducer()
    {
        return kafkaCreator.BuildProducer<string, SaveAuction>();
    }

    /// <summary>
    /// Shared by the concurrent downloads of one update round. Without it the first download that reads a new snapshot
    /// cancels the others, with it every download that gets the snapshot reads its body and each auction is handed over
    /// by whichever body has it first.
    /// </summary>
    public class PageRace
    {
        private int firstHandOver;
        private int finishedBy;
        private long bodyStart;
        private long bodyEnd;
        private TimeSpan cpuStart;
        private TimeSpan cpuEnd;
        private TimeSpan gcStart;
        private TimeSpan gcEnd;
        /// <summary>
        /// Whether the downloads of this round use a connection each
        /// </summary>
        public bool OwnConnections { get; init; }

        /// <summary>
        /// Time from the first hand over to the finish of the round, zero when the round had no hand over or did not finish
        /// </summary>
        public TimeSpan BodyWall => Volatile.Read(ref bodyEnd) == 0 ? TimeSpan.Zero : Stopwatch.GetElapsedTime(bodyStart, bodyEnd);

        /// <summary>
        /// Garbage collection pause of the whole process during <see cref="BodyWall"/>
        /// </summary>
        public TimeSpan BodyGcPause => Volatile.Read(ref bodyEnd) == 0 ? TimeSpan.Zero : gcEnd - gcStart;

        /// <summary>
        /// Cpu time of the whole process during <see cref="BodyWall"/>
        /// </summary>
        public TimeSpan BodyCpu => Volatile.Read(ref bodyEnd) == 0 ? TimeSpan.Zero : cpuEnd - cpuStart;
        /// <summary>
        /// Cancelled when one download has read its new auctions to the end, stops the bodies of the others
        /// </summary>
        public CancellationTokenSource Finished { get; } = new();

        /// <summary>
        /// True for the one download that hands over the first auction of the round
        /// </summary>
        internal bool TryClaimFirstHandOver()
        {
            if (Interlocked.CompareExchange(ref firstHandOver, 1, 0) != 0)
                return false;
            bodyStart = Stopwatch.GetTimestamp();
            cpuStart = Environment.CpuUsage.TotalTime;
            gcStart = GC.GetTotalPauseDuration();
            return true;
        }

        /// <summary>
        /// True for the one download that finishes the round
        /// </summary>
        internal bool TryClaimFinish()
        {
            if (Interlocked.CompareExchange(ref finishedBy, 1, 0) != 0)
                return false;
            cpuEnd = Environment.CpuUsage.TotalTime;
            gcEnd = GC.GetTotalPauseDuration();
            // written last, readers treat a set end as the round being measured
            Volatile.Write(ref bodyEnd, Stopwatch.GetTimestamp());
            return true;
        }

        /// <summary>
        /// True once one download has read its new auctions to the end
        /// </summary>
        public bool IsFinished => Volatile.Read(ref finishedBy) == 1;
    }

    /// <summary>
    /// Waits for all downloads of a round. A download that failed only fails the round when no other one finished it.
    /// </summary>
    protected static async Task<(DateTime, int)[]> AwaitRace(PageRace race, IReadOnlyCollection<Task<(DateTime, int)>> downloads)
    {
        try
        {
            return await Task.WhenAll(downloads).ConfigureAwait(false);
        }
        catch when (race.IsFinished && downloads.Any(d => d.Status == TaskStatus.RanToCompletion))
        {
            foreach (var failed in downloads.Where(d => d.IsFaulted))
                dev.Logger.Instance.Error(failed.Exception.GetBaseException(), "download failed in a round another one finished");
            return downloads.Where(d => d.Status == TaskStatus.RanToCompletion).Select(d => d.Result).ToArray();
        }
    }

    protected async Task<(DateTime, int)> GetAndSavePage(int pageId, IProducer<string, SaveAuction> p, DateTime lastUpdate, Activity siteSpan, CancellationTokenSource pageUpdate = null, int iter = 0, PageRace race = null)
    {
        await Task.Delay(iter * 200);
        if (pageUpdate.Token.IsCancellationRequested)
            return (lastUpdate, -1);
        var time = lastUpdate.ToUnix() * 1000;
        var page = new AuctionPage();
        var client = GetClient();
        var count = 0;
        var tryCount = 0;
        var oldPageCount = 0;
        var index = 0;
        var age = 0;
        string uuid = null;
        var overallUpdateCancle = new CancellationTokenSource(20000);
        if (pageUpdate != null)
            overallUpdateCancle = pageUpdate;
        var start = DateTime.Now;
        var dnsName = System.Net.Dns.GetHostName();
        var minModTime = GetMinModifiedTime(lastUpdate, lastReceived);
        var roundFinished = false;
        // the connection is chosen per round, without a race there is only the shared one
        var sendClient = race?.OwnConnections == true ? ownClients.GetOrAdd(iter, _ => CreatePageClient()) : httpClient;
        while (page.LastUpdated <= lastUpdate && !overallUpdateCancle.Token.IsCancellationRequested)
        {
            var downloadStart = DateTime.Now;
            //DateTimeOffset.Parse("Tue, 11 Jan 2022 09:37:58 GMT") + TimeSpan.FromSeconds(5);
            var url = ApiBaseUrl + "/v2/skyblock/auctions";
            if (iter >= 1 || pageId != 0)
                url += "?page=" + pageId;
            if (iter == 4)
                url += "&t=" + tryCount + dnsName;
            var message = new HttpRequestMessage(HttpMethod.Get, url);
            // DefaultRequestVersion only applies to convenience methods, has to be set per message
            message.Version = System.Net.HttpVersion.Version20;
            message.VersionPolicy = HttpVersionPolicy.RequestVersionOrLower;
            message.Headers.IfModifiedSince = minModTime;
            if (iter == 2)
                message.Headers.CacheControl = new System.Net.Http.Headers.CacheControlHeaderValue()
                {
                    NoCache = true,
                    MustRevalidate = true,
                    MaxAge = TimeSpan.FromSeconds(0)
                };
            message.Headers.From = "Coflnet";
            CancellationTokenSource tokenSource;
            HttpResponseMessage s;
            try
            {
                // create new token with timeout
                tokenSource = new CancellationTokenSource();
                overallUpdateCancle.Token.Register(tokenSource.Cancel);
                s = await NewMethod(tokenSource, message, sendClient).ConfigureAwait(false);
                tokenSource.Cancel();
            }
            catch (TaskCanceledException)
            {
                Console.Write("canceled");
                return (lastUpdate, 0); // was not fast enough
            }
            var intermediate = DateTime.Now;
            if (s.StatusCode != System.Net.HttpStatusCode.OK || s.Headers.Date == sendClient.DefaultRequestHeaders.IfModifiedSince)
            {
                if (tryCount++ % 20 == 1)
                    Console.WriteLine($" - 20x not changed {pageId} {s.StatusCode}");
                await Task.Delay(5);
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

            using (var bodyStream = await s.Content.ReadAsStreamAsync().ConfigureAwait(false))
            {
                var parser = new AuctionPageStream(bodyStream);
                try
                {
                    page._lastUpdated = await parser.ReadLastUpdated(race?.Finished.Token ?? default).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (race != null && race.Finished.IsCancellationRequested)
                {
                    // another download is done before the start of this body arrived
                    return (lastUpdate, 0);
                }
                if (page.LastUpdated <= lastUpdate)
                {
                    // cloudflare stored the snapshot we already have again, only ask for copies newer than this one
                    minModTime = NewerValidator(minModTime, s.Content.Headers.LastModified, lastUpdate);
                    tryCount++;
                    oldPageCount++;
                    if (oldPageCount > 50)
                    {
                        // give up and retry next minute
                        return (lastUpdate, 0);
                    }
                    siteSpan?.SetTag("try", tryCount);
                    Console.WriteLine("backoff" + oldPageCount);
                    // wait for the server cache to refresh
                    await Task.Delay(REQUEST_BACKOF_DELAY * oldPageCount);
                    continue;
                }
                if (overallUpdateCancle.IsCancellationRequested)
                {
                    pageUpdate.Cancel();
                    return (lastUpdate, 0);
                }
                // with a race the other downloads keep running, the one that is done cancels them
                if (race == null)
                    overallUpdateCancle.Cancel();
                var raceToken = race?.Finished.Token ?? default;
                var endedByItself = true;
                var pagUpdatedAt = page.LastUpdated;
                try
                {
                    await foreach (var auction in parser.ReadAuctions(raceToken).ConfigureAwait(false))
                    {
                        raceToken.ThrowIfCancellationRequested();
                        index++;
                        if (auction.Start < lastUpdate - PublishDelay || !seenAuctions.FirstSighting(auction.Uuid, pagUpdatedAt))
                        {
                            if (index % 250 == 0)
                            {
                                Console.WriteLine($"skipping {auction.Uuid} {auction.Start} {pageId} {index} {count}");
                                // another download handing over an auction first also counts as skipped, only an old one ends a race
                                if (race == null ? index - count > 10 : auction.Start < lastUpdate - PublishDelay)
                                    break;
                            }
                            continue;
                        }
                        using var prodSpan = activitySource.CreateActivity("Prod", ActivityKind.Server)?.Start();
                        try
                        {
                            FoundNew(pageId, p, page, tryCount, auction, prodSpan, count);
                        }
                        catch (Exception e)
                        {
                            Logger.Instance.Error(e, "Handing new auction");
                        }
                        if (count == 0 && (race == null || race.TryClaimFirstHandOver()))
                        {
                            var updatedAt = s.Headers.Where(h => h.Key.ToLower() == "date").Select(h => h.Value).FirstOrDefault()?.FirstOrDefault();
                            var tage = s.Headers.Where(h => h.Key.ToLower() == "age").Select(h => h.Value).FirstOrDefault()?.FirstOrDefault();
                            Console.WriteLine($"\nFound first new {auction.Uuid} {auction.Start} {pageId}\t tries:{tryCount}\t i: {iter}");

                            // last "auction found" start
                            Updater.LastPullComplete = intermediate;
                            Updater.LastPull = (page._lastUpdated / 1000).ThisIsNowATimeStamp();
                            firstByteTime.Set((downloadStart - pagUpdatedAt).TotalSeconds);
                            firstParsed.Set((intermediate - pagUpdatedAt).TotalSeconds);
                            Console.WriteLine($"dls:{downloadStart.Second}.{downloadStart.Millisecond} \tparse:{intermediate.Second}.{intermediate.Millisecond} \tnow: {DateTime.Now.Second}.{DateTime.Now.Millisecond} ({DateTime.Now - page.LastUpdated})  by{iter}\n{updatedAt} {tage}{(DateTime.Now - start)}");
                        }
                        uuid = auction.Uuid;

                        count++;
                    }
                }
                catch (OperationCanceledException) when (race != null && raceToken.IsCancellationRequested)
                {
                    // another download is done, everything of this snapshot was handed over by then
                    endedByItself = false;
                }
                // once per round, by the download that finished it
                var finishedRound = race == null;
                if (race != null && endedByItself)
                {
                    finishedRound = race.TryClaimFinish();
                    race.Finished.Cancel();
                    overallUpdateCancle.Cancel();
                }
                if (finishedRound)
                {
                    pageOneDone.Set((DateTime.UtcNow - pagUpdatedAt).TotalSeconds);
                    // Last-Modified of the cached copy is when the origin finished sending it, up to 7 seconds after the first byte
                    lastReceived = DateTime.Now;
                    seenAuctions.Forget(pagUpdatedAt - TimeSpan.FromMinutes(3));
                    Console.WriteLine($"done: {DateTime.Now.Second}.{DateTime.Now.Millisecond.ToString("000")}");
                }
                roundFinished = finishedRound;
            }
            LogHeaderName(siteSpan, s, "age");
            LogHeaderName(siteSpan, s, "date");
            LogHeaderName(siteSpan, s, "cf-ray");
            siteSpan.SetTag("host", System.Net.Dns.GetHostName());

            _ = int.TryParse(s.Headers.Where(h => h.Key.ToLower() == "age").Select(h => h.Value).FirstOrDefault()?.FirstOrDefault(), out age);

            Console.WriteLine($"Loaded page: {pageId} found {count} ({uuid}) on {DateTime.Now} {DateTime.Now.Millisecond} update: {page.LastUpdated}{(race == null ? "" : " by" + iter + $" read {index} head {(intermediate - page.LastUpdated).TotalSeconds:0.000} http{s.Version} {(race.OwnConnections ? "own" : "shared")}")}");
            if (roundFinished)
                UpdateDone();
        }
        return (LatestUpdate(page.LastUpdated, lastUpdate), age);
    }

    /// <summary>
    /// The If-Modified-Since value for the next poll: one second after the previous update finished downloading.
    /// Cloudflare reports the time it stored a snapshot as Last-Modified. The copy we already have was stored
    /// by the time we had all of it and answers 304, the next snapshot is fetched a minute later and always passes.
    /// A fixed offset from the stamp answers 304 for the whole minute whenever the next snapshot is fetched within that offset.
    /// </summary>
    internal static DateTimeOffset GetMinModifiedTime(DateTime lastUpdate, DateTime lastReceived)
    {
        // nothing received for this stamp yet (startup)
        var received = lastReceived < lastUpdate ? lastUpdate : lastReceived;
        var afterReceive = received + TimeSpan.FromSeconds(1);
        // the next snapshot is stamped 60 seconds later and can not be fetched before that
        var beforeNextStamp = lastUpdate + TimeSpan.FromSeconds(59);
        return new DateTimeOffset(afterReceive < beforeNextStamp ? afterReceive : beforeNextStamp);
    }

    /// <summary>
    /// A copy of the snapshot we already have that was stored later than <see cref="GetMinModifiedTime"/> assumed answers 200
    /// with the full page on every poll. Its own Last-Modified becomes the new If-Modified-Since so the next polls get a 304 again.
    /// Never later than a second before the next stamp, otherwise the next snapshot could answer 304 as well.
    /// </summary>
    internal static DateTimeOffset NewerValidator(DateTimeOffset current, DateTimeOffset? lastModifiedOfOldCopy, DateTime lastUpdate)
    {
        if (lastModifiedOfOldCopy == null || lastModifiedOfOldCopy <= current)
            return current;
        var beforeNextStamp = new DateTimeOffset(lastUpdate + TimeSpan.FromSeconds(59));
        return lastModifiedOfOldCopy < beforeNextStamp ? lastModifiedOfOldCopy.Value : beforeNextStamp;
    }

    /// <summary>
    /// A poll that ran into its timeout has no page and must not move the last update back to 1970,
    /// that would report the complete first page as new on the next run
    /// </summary>
    internal static DateTime LatestUpdate(DateTime pageUpdated, DateTime lastUpdate)
    {
        return pageUpdated > lastUpdate ? pageUpdated : lastUpdate;
    }

    protected virtual void UpdateDone()
    {
    }

    protected virtual void FoundNew(int pageId, IProducer<string, SaveAuction> p, AuctionPage page, int tryCount, Auction auction, Activity prodSpan, int count)
    {
        var a = Updater.ConvertAuction(auction, page.LastUpdated);
        a.Context["upage"] = pageId.ToString();
        a.Context["utry"] = tryCount.ToString();
        a.Context["ucount"] = count.ToString();
        Updater.ProduceIntoTopic(Updater.NewAuctionsTopic, p, a, prodSpan);
    }

    protected virtual async Task<HttpResponseMessage> NewMethod(CancellationTokenSource tokenSource, HttpRequestMessage message, HttpClient client)
    {
        return await client.SendAsync(message, HttpCompletionOption.ResponseHeadersRead, tokenSource.Token).ConfigureAwait(false);
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

