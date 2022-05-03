using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Coflnet.Sky.Updater.Models;
using Confluent.Kafka;
using Coflnet.Sky.Core;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Newtonsoft.Json;
using RestSharp;

namespace Coflnet.Sky.Updater
{
    /// <summary>
    /// Checks if missing auctions were sold or canceled
    /// </summary>
    public class MissingChecker : BackgroundService
    {
        private IConfiguration config;
        private string apiKey;


        private static ProducerConfig producerConfig = new ProducerConfig
        {
            BootstrapServers = SimplerConfig.Config.Instance["KAFKA_HOST"],
            LingerMs = 100,
        };

        public MissingChecker(IConfiguration config)
        {
            this.config = config;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var keys = config["API_KEY"]?.Split(",");
            if (keys == null || keys.Length <= Updater.updaterIndex)
                return; // no key for this instance
            apiKey = keys[Updater.updaterIndex];
            await Kafka.KafkaConsumer.ConsumeBatch<SaveAuction>(
                config["KAFKA_HOST"],
                config["TOPICS:AUCTION_CHECK"],
                async auctions =>
                {
                    try
                    {
                        // prefilter
                        var toCheck = auctions.Where(a => a.End > DateTime.UtcNow - TimeSpan.FromHours(1)).ToList();
                        if (toCheck.Count == 0)
                        {
                            Console.WriteLine($"skipped batch because to old {auctions.FirstOrDefault()?.Uuid} " + auctions.FirstOrDefault()?.End);
                            return;
                        }
                        Console.WriteLine("check batch info: " + auctions.FirstOrDefault()?.Uuid + auctions.FirstOrDefault()?.End);
                        await CheckBatch(auctions);
                    }
                    catch (Exception e)
                    {
                        dev.Logger.Instance.Error(e, "checking missing");
                        await Task.Delay(30000);
                    }
                }, stoppingToken, "sky-updater", 50);
        }

        private async Task CheckBatch(IEnumerable<SaveAuction> auctions)
        {
            Console.WriteLine("got a batch" + auctions.Count());
            var ids = auctions.GroupBy(a => a.AuctioneerId).Select(a => a.First().AuctioneerId).ToList();
            var client = new RestClient("https://api.hypixel.net/skyblock/");

            var start = DateTime.Now;
            using (var p = new ProducerBuilder<string, SaveAuction>(producerConfig).SetValueSerializer(SerializerFactory.GetSerializer<SaveAuction>()).Build())
            {
                await Task.WhenAll(ids.Select(async id =>
                {
                    try
                    {
                        var request = new RestRequest($"auction?key={apiKey}&player={id}", Method.GET);

                        //Get the response and Deserialize
                        var response = await client.ExecuteAsync(request).ConfigureAwait(false);
                        if (response.StatusCode != System.Net.HttpStatusCode.OK)
                            return;
                        var responseDeserialized = JsonConvert.DeserializeObject<AuctionsByPlayer>(response?.Content);
                        foreach (var auction in responseDeserialized.Auctions)
                        {
                            var target = auctions.Where(a => a.Uuid == auction.Uuid).FirstOrDefault();
                            var item = Updater.ConvertAuction(auction);
                            if (item.Start > DateTime.UtcNow - TimeSpan.FromMinutes(2))
                                Updater.ProduceIntoTopic(Updater.NewAuctionsTopic, p, item, null);
                            else if (target?.HighestBidAmount > auction.HighestBidAmount)
                            {
                                Updater.ProduceIntoTopic(Updater.NewBidsTopic, p, item, null);
                                Console.WriteLine($"found new bid {item.Uuid} {auction.HighestBidAmount} {auction.Bids.FirstOrDefault().Timestamp}");
                            }
                            else
                                Updater.ProduceIntoTopic(Updater.AuctionEndedTopic, p, item, null);

                        }
                    }
                    catch (Exception e)
                    {
                        dev.Logger.Instance.Error(e, "getting player auctions " + id);
                    }

                }));

                // wait for up to 10 seconds for any inflight messages to be delivered.
                p.Flush(TimeSpan.FromSeconds(10));
            }
            var delay = start + TimeSpan.FromSeconds(60) - DateTime.Now;
            dev.Logger.Instance.Info("players " + ids.Count());
            if (delay < TimeSpan.Zero)
                return;
            await Task.Delay(delay);
        }

        public class AuctionsByPlayer
        {
            [JsonProperty("auctions")]
            public List<PlayerAuction> Auctions { get; set; }
        }

        public class PlayerAuction : Auction
        {

            [Newtonsoft.Json.JsonProperty("item_bytes")]
            public ItemBytesWithType ItemBytesObj { get; set; }
            [JsonIgnore]
            public override string ItemBytes { get => this.ItemBytesObj.Data; set => base.ItemBytes = value; }
        }

        public class ItemBytesWithType
        {
            public int Type { get; set; }
            public string Data { get; set; }
        }

    }
}
