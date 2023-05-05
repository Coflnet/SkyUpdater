using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Coflnet.Sky.Items.Client.Api;
using System.Collections.Concurrent;
using Coflnet.Sky.Updater.Models;
using Coflnet.Sky.Core;
using System.Diagnostics;

namespace Coflnet.Sky.Updater;

public interface IItemSkinHandler
{
    void StoreIfNeeded(SaveAuction parsed, Auction auction);
}

public class ItemSkinHandler : BackgroundService, IItemSkinHandler
{
    private Sky.Items.Client.Api.IItemsApi itemsApi;
    private ConcurrentDictionary<string, bool> skinNames = new();
    private ActivitySource activitySource;

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
                using var activity = activitySource?.StartActivity("UpdateSkins");
                var items = await itemsApi.ItemsNoiconGetAsync(0, stoppingToken);
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
        if (!skinNames.TryGetValue(tag, out var saved) && saved)
            return;
        skinNames[tag] = true;
        Task.Run(async () =>
        {
            try
            {
                var skullUrl = NBT.SkullUrl(auction.ItemBytes);
                if (skullUrl == null)
                    return;
                await itemsApi.ItemItemTagTexturePostAsync(tag, tag, skullUrl);
                Console.WriteLine($"updated skin for {tag}");
            }
            catch (Exception e)
            {
                dev.Logger.Instance.Error(e, "loading skin for " + tag);
            }
        });
    }
}
