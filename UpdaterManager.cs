using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Coflnet.Sky.Core;
using System.Diagnostics;
using Coflnet.Kafka;

namespace Coflnet.Sky.Updater
{
    public class UpdaterManager : BackgroundService
    {
        ItemSkinHandler skinHandler;
        ActivitySource activitySource;
        KafkaCreator kafkaCreator;
        public UpdaterManager(ItemSkinHandler skinHandler, ActivitySource activitySource, KafkaCreator kafkaCreator)
        {
            this.skinHandler = skinHandler;
            this.activitySource = activitySource;
            this.kafkaCreator = kafkaCreator;
        }
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var bazzar = new BazaarUpdater();
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