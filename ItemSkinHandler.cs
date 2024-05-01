using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Coflnet.Sky.Items.Client.Api;
using System.Collections.Concurrent;
using Coflnet.Sky.Updater.Models;
using Coflnet.Sky.Core;
using System.Diagnostics;
using fNbt;
using fNbt.Tags;

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

    public ItemSkinHandler(IItemsApi itemsApi, ActivitySource activitySource)
    {
        this.itemsApi = itemsApi;
        this.activitySource = activitySource;
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
                items.Clear();
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
        if (!skinNames.TryGetValue(tag, out var saved) || saved)
            return;
        skinNames[tag] = true;
        Task.Run(async () =>
        {
            try
            {
                Console.WriteLine($"loading skin for {tag}");
                var skullUrl = NBT.SkullUrl(auction.ItemBytes);
                if (skullUrl == null)
                {
                    Console.WriteLine($"no skin found for {tag} {auction.ItemBytes}");

                    var root = NBT.File(Convert.FromBase64String(auction.ItemBytes)).RootTag.Get<NbtList>("i")
                    .Get<NbtCompound>(0);
                    var id = root.Get("id").ShortValue;
                    var meta = root.Get("Damage").ShortValue;
                    var displayTag = MinecraftTypeParser.Instance.ItemTagFromId(id, meta);
                    var url = $"https://sky.coflnet.com/static/icon/{displayTag}";
                    Console.WriteLine($"trying {url} for {tag} {id}");
                    if (displayTag != null)
                        await itemsApi.ItemItemTagTexturePostAsync(tag, url);
                    //skinNames[tag] = false;
                    return;
                }
                await itemsApi.ItemItemTagTexturePostAsync(tag, skullUrl);
                Console.WriteLine($"updated skin for {tag} to {skullUrl}");
            }
            catch (Exception e)
            {
                dev.Logger.Instance.Error(e, "loading skin for " + tag);
            }
        });
    }
}
