using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Coflnet.Sky.Core;
using Hypixel.NET;
using Hypixel.NET.SkyblockApi;
using Newtonsoft.Json;
using Prometheus;
using RestSharp;

namespace Coflnet.Sky.Updater;

public class BinUpdater
{
    private List<string> apiKeys = new List<string>();
    private static string LastFirst = null;
    static Gauge binSells = Metrics.CreateGauge("sky_auction_ended", "The amount of sold or ended auctions");

    public BinUpdater(IEnumerable<string> apiKeys)
    {
        this.apiKeys.AddRange(apiKeys);
        historyLimit = DateTime.Now - TimeSpan.FromHours(1);
    }

    private DateTime historyLimit;

    public static async Task<List<SaveAuction>> GrabAuctions(HypixelApi hypixelApi)
    {
        using var span = Updater.activitySource.CreateActivity("SoldAuctions", System.Diagnostics.ActivityKind.Server)?.Start();
        List<SaveAuction> auctions = await DownloadSells("https://api.hypixel.net");
        //Updater.AddSoldAuctions(auctions, span);

        return auctions;
    }

    public static async Task<List<SaveAuction>> DownloadSells(string BaseUrl)
    {
        string firstFoundUuid = null;
        List<SaveAuction> auctions = new List<SaveAuction>();
        var tries = 0;
        while ((firstFoundUuid == LastFirst || auctions.Count == 0) && tries++ < 5)
        {
            auctions = await DownloadAndParse(BaseUrl).ConfigureAwait(false);
            firstFoundUuid = auctions.FirstOrDefault()?.Uuid;
            if (firstFoundUuid == LastFirst)
            {
                Console.WriteLine("No new auctions found, waiting 1s");
                await Task.Delay(1000);
            }
        }
        if (Updater.updaterIndex % 2 == 0)
            Console.WriteLine($"Updated {auctions.Count} bin sells eg {firstFoundUuid}");
        else
            Console.WriteLine($"Updated {auctions.Count} bin sells {string.Join(',', auctions.Select(a => a.Uuid))}");
        LastFirst = firstFoundUuid;
        binSells.Set(auctions.Count);
        return auctions;
    }

    private static async Task<List<SaveAuction>> DownloadAndParse(string BaseUrl)
    {
        var client = new RestClient(BaseUrl);
        var request = new RestRequest($"/v2/skyblock/auctions_ended?cache=" + System.Net.Dns.GetHostName(), Method.Get);

        //Get the response and Deserialize
        var response = await client.ExecuteAsync(request).ConfigureAwait(false);
        var expired = JsonConvert.DeserializeObject<AuctionsEnded>(response.Content);
        var auctions = expired.Auctions.Select(item =>
        {
            var a = new SaveAuction()
            {
                Uuid = item.Uuid,
                AuctioneerId = item.Seller,
                ProfileId = item.ProfileId,
                Bids = new List<SaveBids>()
                    {
                            new SaveBids()
                            {
                                Amount = item.Price,
                                    Bidder = item.Buyer,
                                    Timestamp = item.TimeStamp,
                                    ProfileId = item.BuyerProfile
                            }
                    },
                HighestBidAmount = item.Price,
                Bin = item.BuyItemNow,
                End = item.TimeStamp,
                UId = AuctionService.Instance.GetId(item.Uuid)
            };
            NBT.FillDetails(a, item.ItemBytes, true);
            if (a.Tier == Tier.UNKNOWN)
                Console.WriteLine("unkown tier" + NBT.Pretty(item.ItemBytes));

            return a;
        }).ToList();
        if (auctions.Count == 0)
        {
            Console.WriteLine("No auctions found");
            Console.WriteLine(response.Content);
        }
        return auctions;
    }
}
