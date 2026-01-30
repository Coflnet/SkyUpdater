using System;
using System.Collections.Generic;
using System.Linq;
using Coflnet.Sky.Core;
using Hypixel.NET;
using System.Threading.Tasks;
using Confluent.Kafka;
using dev;
using Coflnet.Kafka;

namespace Coflnet.Sky.Updater;
public class BazaarUpdater
{
    private bool abort;

    public static DateTime LastUpdate { get; internal set; }

    public static Dictionary<string, QuickStatus> LastStats = new Dictionary<string, QuickStatus>();

    public static readonly string KafkaTopic = SimplerConfig.Config.Instance["TOPICS:BAZAAR"];

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
            await ProduceIntoQueue(pull);
            Console.WriteLine($"Bazaar updated {pull.Products.Count} items eg {pull.Products.First().ProductId} at {result.LastUpdated} ({DateTime.UtcNow}) tries {tryCount}");
            return result.LastUpdated;
        }
        
        // Give up after max retries, log it and return unchanged
        Console.WriteLine($"Bazaar update gave up after {maxRetries} tries at {DateTime.Now}, no update available");
        return lastUpdate;
    }

    public void UpdateForEver(string apiKey)
    {
        HypixelApi api = null;
        Task.Run(async () =>
        {
            int i = 0;
            var lastUpdate = DateTime.Now - TimeSpan.FromMinutes(2);
            while (!abort)
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
        }).ConfigureAwait(false); ;
    }

    private Kafka.KafkaCreator kafkaCreator;

    public BazaarUpdater(Kafka.KafkaCreator kafkaCreator)
    {
        this.kafkaCreator = kafkaCreator;
    }

    protected virtual Task ProduceIntoQueue(BazaarPull pull)
    {
        using (var p = kafkaCreator.BuildProducer<string, BazaarPull>())
        {
            p.Produce(KafkaTopic, new Message<string, BazaarPull> { Value = pull, Key = pull.Timestamp.ToString() }, handler =>
            {
                Console.WriteLine("wrote bazaar log " + handler.TopicPartitionOffset.Offset);
            });
            p.Flush(TimeSpan.FromSeconds(10));
            return Task.CompletedTask;
        }
    }

    internal void Stop()
    {
        abort = true;
    }
}