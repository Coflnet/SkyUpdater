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
    protected int SecondsBetweenUpdates = 10;

    private async Task PullAndSave(HypixelApi api, int i)
    {
        var result = await api.GetBazaarProductsAsync();
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
    }

    private async Task WaitForServerCacheRefresh(int i, DateTime start)
    {
        var timeToSleep = start.Add(TimeSpan.FromSeconds(SecondsBetweenUpdates)) - DateTime.Now;
        Console.Write($"\r {i} {timeToSleep}");
        if (timeToSleep.Seconds > 0)
            await Task.Delay(timeToSleep);
    }

    public void UpdateForEver(string apiKey)
    {
        HypixelApi api = null;
        Task.Run(async () =>
        {
            int i = 0;
            while (!abort)
            {
                try
                {
                    if (api == null)
                        api = new HypixelApi(apiKey, 9);
                    var start = DateTime.Now;
                    await PullAndSave(api, i);
                    await WaitForServerCacheRefresh(i, start);
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