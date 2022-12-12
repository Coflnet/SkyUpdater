using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Coflnet.Sky.Items.Client.Api;
using System.Collections.Concurrent;
using Coflnet.Sky.Updater.Models;
using Coflnet.Sky.Core;

namespace Coflnet.Sky.Updater;
public class ItemSkinHandler : BackgroundService
{
    private Sky.Items.Client.Api.IItemsApi itemsApi;
    private ConcurrentDictionary<string, bool> skinNames = new();

    public ItemSkinHandler(IItemsApi itemsApi)
    {
        this.itemsApi = itemsApi;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var items = await itemsApi.ItemsNoiconGetAsync(0, stoppingToken);
                skinNames.Clear();
                foreach (var item in items)
                {
                    skinNames.TryAdd(item.Tag, false);
                }
                Console.WriteLine($"found {skinNames.Count} items without skins");
            }
            catch (Exception e)
            {
                dev.Logger.Instance.Error(e, "updating skins");
            }
            await Task.Delay(TimeSpan.FromMinutes(1), stoppingToken);
        }
    }

    public void StoreIfNeeded(SaveAuction parsed, Auction auction)
    {
        var tag = parsed.Tag;
        if (tag == null)
            return;
        if (!skinNames.TryGetValue(tag, out var saved) && !saved)
            return;
        Console.WriteLine($"found skin for {tag}");
        Task.Run(async () =>
        {
            try
            {
                skinNames[tag] = true;
                await itemsApi.ItemItemTagTexturePostAsync(tag, tag, NBT.SkullUrl(auction.ItemBytes));
                Console.WriteLine($"updated skin for {tag}");
            }
            catch (Exception e)
            {
                dev.Logger.Instance.Error(e, "updating skin");
            }
        });
    }
}
