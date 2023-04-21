using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Coflnet.Sky.Core;
using System.Diagnostics;
using Coflnet.Kafka;
using Coflnet.Sky.Updater.Models;

namespace Coflnet.Sky.Updater
{
    public class UpdaterManager : BackgroundService
    {
        ItemSkinHandler skinHandler;
        ActivitySource activitySource;
        KafkaCreator kafkaCreator;
        Topics topics;
        public UpdaterManager(ItemSkinHandler skinHandler, ActivitySource activitySource, KafkaCreator kafkaCreator, Topics topics)
        {
            this.skinHandler = skinHandler;
            this.activitySource = activitySource;
            this.kafkaCreator = kafkaCreator;
            this.topics = topics;
        }
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            await kafkaCreator.CreateTopicIfNotExist(topics.AhSumary, 2);
            await kafkaCreator.CreateTopicIfNotExist(topics.Bazaar, 2);
            await kafkaCreator.CreateTopicIfNotExist(topics.MissingAuction);
            await kafkaCreator.CreateTopicIfNotExist(topics.NewAuction);
            await kafkaCreator.CreateTopicIfNotExist(topics.NewBid);
            await kafkaCreator.CreateTopicIfNotExist(topics.SoldAuction);
            await kafkaCreator.CreateTopicIfNotExist(topics.AuctionEnded);
            await kafkaCreator.CreateTopicIfNotExist(topics.AuctionCheck);
            
            var bazzar = new BazaarUpdater(kafkaCreator);
            var updater = new Updater(null, skinHandler, activitySource, kafkaCreator);
            var loading = ItemDetails.Instance.LoadFromDB();

            if (!Int32.TryParse(System.Net.Dns.GetHostName().Split('-').Last(), out Updater.updaterIndex))
                Updater.updaterIndex = 0;

            if (Updater.updaterIndex % 2 == 0)
                bazzar.UpdateForEver(null);
            updater.UpdateForEver();
            try
            {
                await loading;
            }
            catch (Exception e)
            {
                Console.WriteLine("-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|");
                Console.WriteLine($"Failed to load items {e.Message}\n {e.StackTrace}");
            }
        }
    }
}