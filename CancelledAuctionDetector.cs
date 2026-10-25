using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Coflnet.Sky.Core;
using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Prometheus;

namespace Coflnet.Sky.Updater;

/// <summary>
/// Detects auctions that we still hold as active but that are gone from a fresh hypixel
/// seller listing (cancelled by the seller) and produces them to the cancelled topic.
/// </summary>
public class CancelledAuctionDetector
{
    private const int MaxOursSanity = 500;
    private static readonly TimeSpan CacheDuration = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan RequestTimeout = TimeSpan.FromSeconds(2);
    static readonly Counter detected = Metrics.CreateCounter("sky_updater_cancelled_detected", "auctions detected as cancelled");

    private readonly HttpClient client;
    private readonly ILogger<CancelledAuctionDetector> logger;
    private readonly string baseUrl;
    private readonly string topic;
    private readonly ConcurrentDictionary<string, (DateTime expires, long[] uids)> cache = new();

    public CancelledAuctionDetector(HttpClient client, IConfiguration config, ILogger<CancelledAuctionDetector> logger)
    {
        this.client = client;
        this.logger = logger;
        baseUrl = (config["INDEXER_BASE_URL"] ?? "http://sky-indexer:8000").TrimEnd('/');
        topic = config["TOPICS:MISSING_AUCTION"] ?? Updater.MissingAuctionsTopic;
    }

    public async Task<int> DetectAndProduceAsync(string playerId, IReadOnlyCollection<SaveAuction> hypixelAuctions,
        IProducer<string, SaveAuction> producer, string hintSource, CancellationToken ct)
    {
        var ours = await GetOursAsync(playerId, ct).ConfigureAwait(false);
        if (ours == null || ours.Length == 0 || ours.Length > MaxOursSanity)
            return 0;
        var hypixel = hypixelAuctions
            .Select(a => a.UId != 0 ? a.UId : AuctionService.Instance.GetId(a.Uuid))
            .ToHashSet();
        var cancelled = ours.Where(u => !hypixel.Contains(u)).Distinct().ToList();
        foreach (var uid in cancelled)
        {
            string uuid = null;
            try { uuid = AuctionService.Instance.GetUuid(uid); } catch (Exception) { }
            var stub = new SaveAuction
            {
                Uuid = uuid,
                UId = uid,
                AuctioneerId = playerId,
                End = DateTime.UtcNow,
                Bids = new List<SaveBids>(),
                Context = new Dictionary<string, string> { ["cancelled"] = hintSource }
            };
            Updater.ProduceIntoTopic(topic, producer, stub, null);
            detected.Inc();
        }
        if (cancelled.Count > 0)
            logger.LogInformation("detected {count} cancelled auctions of {player} via {source}", cancelled.Count, playerId, hintSource);
        return cancelled.Count;
    }

    private async Task<long[]> GetOursAsync(string playerId, CancellationToken ct)
    {
        if (cache.TryGetValue(playerId, out var entry) && entry.expires > DateTime.UtcNow)
            return entry.uids;
        try
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            cts.CancelAfter(RequestTimeout);
            using var response = await client.GetAsync($"{baseUrl}/active/seller/{playerId}/uids", cts.Token).ConfigureAwait(false);
            if (response.StatusCode != System.Net.HttpStatusCode.OK)
            {
                logger.LogDebug("indexer returned {status} for {player}", response.StatusCode, playerId);
                return null;
            }
            var body = await response.Content.ReadAsStringAsync(cts.Token).ConfigureAwait(false);
            var uids = JsonSerializer.Deserialize<long[]>(body) ?? Array.Empty<long>();
            if (cache.Count > 5000)
                foreach (var old in cache.Where(c => c.Value.expires < DateTime.UtcNow).Select(c => c.Key).ToList())
                    cache.TryRemove(old, out _);
            cache[playerId] = (DateTime.UtcNow + CacheDuration, uids);
            return uids;
        }
        catch (Exception e)
        {
            logger.LogDebug(e, "indexer unavailable for {player}", playerId);
            return null;
        }
    }
}
