using System;
using System.Collections.Generic;
using System.Linq;
using Coflnet.Sky.Core;
using Hypixel.NET;
using System.Threading.Tasks;
using System.Threading;
using Confluent.Kafka;
using dev;
using Coflnet.Kafka;
using Prometheus;

namespace Coflnet.Sky.Updater;
public class BazaarUpdater : IDisposable
{
    public static DateTime LastUpdate { get; internal set; }

    public static Dictionary<string, QuickStatus> LastStats = new Dictionary<string, QuickStatus>();

    public static readonly string KafkaTopic = SimplerConfig.Config.Instance["TOPICS:BAZAAR"];

    private static readonly Gauge productCount = Metrics.CreateGauge(
        "sky_updater_bazaar_product_count", "Number of products in the latest Bazaar API update");
    private static readonly Gauge missingProductCount = Metrics.CreateGauge(
        "sky_updater_bazaar_missing_product_count", "Products missing compared with the last complete Bazaar API update");
    private static readonly Gauge lastCompletePullTimestamp = Metrics.CreateGauge(
        "sky_updater_bazaar_last_complete_pull_timestamp_seconds", "Timestamp of the latest Bazaar API update with the complete known product set");
    private static readonly Counter incompletePullCount = Metrics.CreateCounter(
        "sky_updater_bazaar_incomplete_pull_total", "Bazaar API updates missing products from the last complete update");
    private static readonly Counter publishFailed = Metrics.CreateCounter(
        "sky_updater_bazaar_publish_failed_total", "Bazaar updates that failed durable Kafka publication");
    private static readonly Histogram publishDuration = Metrics.CreateHistogram(
        "sky_updater_bazaar_publish_duration_seconds", "Time spent durably publishing a Bazaar update to Kafka");

    private readonly HashSet<string> expectedProductIds = new();
    private readonly IProducer<string, BazaarPull> producer;

    private async Task<DateTime> PullAndSave(HypixelApi api, int i, DateTime lastUpdate)
    {
        var tryCount = 0;
        var maxRetries = 100;
        while (tryCount < maxRetries)
        {
            var result = await api.GetBazaarProductsAsync();
            
            // Check if timestamp changed
            if (result.LastUpdated <= lastUpdate)
            {
                tryCount++;
                if (tryCount % 10 == 1)
                    Console.WriteLine($" - Bazaar not updated after {tryCount} attempts, last: {result.LastUpdated}, expected > {lastUpdate}");
                await Task.Delay(500);
                continue;
            }

            var pull = new BazaarPull()
            {
                Timestamp = result.LastUpdated
            };
            pull.Products = result.Products.Select(p =>
            {
                var pInfo = new ProductInfo()
                {
                    ProductId = p.Value.ProductId,
                    BuySummery = p.Value.BuySummary.Select(s => new BuyOrder()
                    {
                        Amount = (int)s.Amount,
                        Orders = (short)s.Orders,
                        PricePerUnit = s.PricePerUnit
                    }).ToList(),
                    SellSummary = p.Value.SellSummary.Select(s => new SellOrder()
                    {
                        Amount = (int)s.Amount,
                        Orders = (short)s.Orders,
                        PricePerUnit = s.PricePerUnit
                    }).ToList(),
                    QuickStatus = new QuickStatus()
                    {
                        ProductId = p.Value.QuickStatus.ProductId,
                        BuyMovingWeek = p.Value.QuickStatus.BuyMovingWeek,
                        BuyOrders = (int)p.Value.QuickStatus.BuyOrders,
                        BuyPrice = p.Value.QuickStatus.BuyPrice,
                        BuyVolume = p.Value.QuickStatus.BuyVolume,
                        SellMovingWeek = p.Value.QuickStatus.SellMovingWeek,
                        SellOrders = (int)p.Value.QuickStatus.SellOrders,
                        SellPrice = p.Value.QuickStatus.SellPrice,
                        SellVolume = p.Value.QuickStatus.SellVolume
                    },
                    PullInstance = pull
                };
                pInfo.QuickStatus.SellPrice = p.Value.SellSummary.Select(o => o.PricePerUnit).FirstOrDefault();
                pInfo.QuickStatus.BuyPrice = p.Value.BuySummary.Select(o => o.PricePerUnit).FirstOrDefault();
                return pInfo;
            }).ToList();
            RecordProductCompleteness(pull);
            await ProduceIntoQueue(pull);
            Console.WriteLine($"Bazaar updated {pull.Products.Count} items eg {pull.Products.First().ProductId} at {result.LastUpdated} ({DateTime.UtcNow}) tries {tryCount}");
            return result.LastUpdated;
        }
        
        // Give up after max retries, log it and return unchanged
        Console.WriteLine($"Bazaar update gave up after {maxRetries} tries at {DateTime.Now}, no update available");
        return lastUpdate;
    }

    public Task UpdateForEver(string apiKey, CancellationToken stoppingToken = default)
    {
        return Task.Run(async () =>
        {
            HypixelApi api = null;
            int i = 0;
            var lastUpdate = DateTime.Now - TimeSpan.FromMinutes(2);
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    if (api == null)
                        api = new HypixelApi(apiKey, 9);
                    
                    // Wait 19.5 seconds after last update before pulling
                    var waitTime = lastUpdate + TimeSpan.FromSeconds(9.5) - DateTime.Now;
                    if (waitTime > TimeSpan.Zero)
                        await Task.Delay(waitTime);
                    
                    var result = await PullAndSave(api, i, lastUpdate);
                    if (result != lastUpdate)
                    {
                        lastUpdate = result;
                    }
                    else
                    {
                        // Update was skipped, wait before retrying
                        await Task.Delay(500);
                    }
                    
                    i++;
                }
                catch (Exception e)
                {
                    Logger.Instance.Error($"\nBazaar update failed {e.Message} \n{e.StackTrace} \n{e.InnerException?.Message}");
                    Console.WriteLine($"\nBazaar update failed {e.Message} \n{e.InnerException?.Message}");
                    await Task.Delay(5000);
                }
            }
            Console.WriteLine("Stopped Bazaar :/");
        });
    }

    public BazaarUpdater(Kafka.KafkaCreator kafkaCreator)
    {
        producer = kafkaCreator?.BuildProducer<string, BazaarPull>();
    }

    private void RecordProductCompleteness(BazaarPull pull)
    {
        var currentProductIds = pull.Products.Select(product => product.ProductId).ToHashSet();
        productCount.Set(currentProductIds.Count);
        if (expectedProductIds.Count == 0)
            expectedProductIds.UnionWith(currentProductIds);

        var missing = expectedProductIds.Except(currentProductIds).ToList();
        missingProductCount.Set(missing.Count);
        if (missing.Count == 0)
        {
            expectedProductIds.UnionWith(currentProductIds);
            lastCompletePullTimestamp.Set(new DateTimeOffset(pull.Timestamp.ToUniversalTime()).ToUnixTimeSeconds());
            return;
        }

        incompletePullCount.Inc();
        Logger.Instance.Error($"Bazaar update {pull.Timestamp:O} is missing {missing.Count} products: {string.Join(',', missing)}");
    }

    protected virtual async Task ProduceIntoQueue(BazaarPull pull)
    {
        if (producer == null)
            throw new InvalidOperationException("No Kafka producer is configured");

        using var timer = publishDuration.NewTimer();
        try
        {
            var result = await producer.ProduceAsync(KafkaTopic,
                new Message<string, BazaarPull> { Value = pull, Key = pull.Timestamp.ToString() });
            if (result.Status != PersistenceStatus.Persisted)
                throw new InvalidOperationException($"Kafka reported Bazaar update persistence status {result.Status}");
            Console.WriteLine("wrote bazaar log " + result.TopicPartitionOffset.Offset);
        }
        catch
        {
            publishFailed.Inc();
            throw;
        }
    }

    public void Dispose()
    {
        producer?.Flush(TimeSpan.FromSeconds(10));
        producer?.Dispose();
    }
}
