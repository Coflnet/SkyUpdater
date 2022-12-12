using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Coflnet.Sky.Core;

namespace Coflnet.Sky.Updater
{
    public class UpdaterManager : BackgroundService
    {
        ItemSkinHandler skinHandler;
        public UpdaterManager(ItemSkinHandler skinHandler)
        {
            this.skinHandler = skinHandler;
        }
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var bazzar = new BazaarUpdater();
            var updater = new Updater(null, skinHandler);
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