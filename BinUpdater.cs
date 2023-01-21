using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Coflnet.Sky.Core;
using Hypixel.NET;
using Hypixel.NET.SkyblockApi;
using Newtonsoft.Json;
using OpenTracing.Util;
using RestSharp;

namespace Coflnet.Sky.Updater
{
    public class BinUpdater
    {
        private List<string> apiKeys = new List<string>();

        /// <summary>
        /// Dictionary of minecraft player ids and how many auctions they had to block pulling their whole history twice
        /// </summary>
        /// <typeparam name="string"></typeparam>
        /// <typeparam name="short"></typeparam>
        /// <returns></returns>
        private static ConcurrentDictionary<uint, short> PulledAlready = new ConcurrentDictionary<uint, short>();

        public static List<SaveAuction> SoldLastMin
        {
            get
            {
                return CacheService.Instance.GetFromRedis<List<SaveAuction>>("endedAuctions").Result;
            }
            set
            {
                CacheService.Instance.SaveInRedis("endedAuctions", value).Wait(5000);
            }
        }

        public BinUpdater(IEnumerable<string> apiKeys)
        {
            this.apiKeys.AddRange(apiKeys);
            historyLimit = DateTime.Now - TimeSpan.FromHours(1);
        }

        private DateTime historyLimit;

        public static async Task GrabAuctions(HypixelApi hypixelApi)
        {
            using var span = GlobalTracer.Instance.BuildSpan("SoldAuctions").StartActive();
            List<SaveAuction> auctions = await DownloadSells("https://api.hypixel.net");
            Updater.AddSoldAuctions(auctions, span);

            SoldLastMin = auctions;

        }

        public static async Task<List<SaveAuction>> DownloadSells(string BaseUrl)
        {
            var client = new RestClient(BaseUrl);
            var request = new RestRequest($"skyblock/auctions_ended", Method.Get);

            //Get the response and Deserialize
            var response = await client.ExecuteAsync(request).ConfigureAwait(false);
            var expired = JsonConvert.DeserializeObject<AuctionsEnded>(response.Content);
            var auctions = expired.Auctions.Select(item =>
            {
                var a = new SaveAuction()
                {
                    Uuid = item.Uuid,
                    AuctioneerId = item.Seller,
                    Bids = new List<SaveBids>()
                        {
                            new SaveBids()
                            {
                                Amount = item.Price,
                                    Bidder = item.Buyer,
                                    Timestamp = item.TimeStamp,
                                    ProfileId = "unknown"
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
            Console.WriteLine($"Updated {expired.Auctions.Count} bin sells eg {expired.Auctions.FirstOrDefault()?.Uuid}");
            return auctions;
        }
    }
}