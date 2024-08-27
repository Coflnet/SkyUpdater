using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Coflnet.Sky.Core;
using System.Diagnostics;
using Coflnet.Kafka;
using Coflnet.Sky.Updater.Models;
using Microsoft.Extensions.Configuration;

namespace Coflnet.Sky.Updater
{
    public class UpdaterManager : BackgroundService
    {
        ItemSkinHandler skinHandler;
        ActivitySource activitySource;
        Kafka.KafkaCreator kafkaCreator;
        Topics topics;
        IConfiguration config;
        public UpdaterManager(ItemSkinHandler skinHandler, ActivitySource activitySource, Kafka.KafkaCreator kafkaCreator, Topics topics, IConfiguration config)
        {
            this.skinHandler = skinHandler;
            this.activitySource = activitySource;
            this.kafkaCreator = kafkaCreator;
            this.topics = topics;
            this.config = config;
        }
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            Console.WriteLine(JSON.Stringify(topics));
            var cc = KafkaCreator.GetClientConfig(config);
            cc.SaslPassword = cc.SaslPassword?.Truncate(3);
            Console.WriteLine(JSON.Stringify(cc));
            await kafkaCreator.CreateTopicIfNotExist(topics.Ah_Sumary, 2);
            await kafkaCreator.CreateTopicIfNotExist(topics.Bazaar, 2);
            await kafkaCreator.CreateTopicIfNotExist(topics.Missing_Auction);
            await kafkaCreator.CreateTopicIfNotExist(topics.New_Auction);
            await kafkaCreator.CreateTopicIfNotExist(topics.New_Bid);
            await kafkaCreator.CreateTopicIfNotExist(topics.Sold_Auction);
            await kafkaCreator.CreateTopicIfNotExist(topics.Auction_Ended);
            await kafkaCreator.CreateTopicIfNotExist(topics.Auction_Check);

            var bazzar = new BazaarUpdater(kafkaCreator);
            var updater = new Updater(null, skinHandler, activitySource, kafkaCreator);
            var loading = ItemDetails.Instance.LoadFromDB();

            if (!Int32.TryParse(System.Net.Dns.GetHostName().Split('-').Last(), out Updater.updaterIndex))
                Updater.updaterIndex = 0;

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