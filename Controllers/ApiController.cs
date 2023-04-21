using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Coflnet.Kafka;
using Coflnet.Sky.Core;
using Coflnet.Sky.Updater;
using Coflnet.Sky.Updater.Models;
using Confluent.Kafka;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;

namespace SkyUpdater.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class ApiController : ControllerBase
    {
        private readonly ILogger<ApiController> _logger;
        private KafkaCreator kafkaCreator;

        public ApiController(ILogger<ApiController> logger, KafkaCreator kafkaCreator)
        {
            _logger = logger;
            this.kafkaCreator = kafkaCreator;
        }


        [Route("time")]
        [HttpGet]
        public DateTime LastUpdate()
        {
            if (Updater.LastPullComplete < DateTime.Now - TimeSpan.FromMinutes(2))
                return default(DateTime);
            return Updater.LastPull + TimeSpan.FromSeconds(10);
        }

        [Route("/skyblock/auctions")]
        [HttpGet]
        [HttpPost]
        [ResponseCache(Duration = 60, Location = ResponseCacheLocation.Any)]
        public string MockAuctions()
        {
            return System.IO.File.ReadAllText("Mock/auctions.json");
        }
        [Route("/skyblock/auction")]
        [HttpPost]
        [ResponseCache(Duration = 60, Location = ResponseCacheLocation.Any)]
        public async Task ProduceMock()
        {
            using var p = kafkaCreator.BuildProducer<string, SaveAuction>();
            var result = await p.ProduceAsync(Updater.NewAuctionsTopic, new Message<string, SaveAuction>
            {
                Key = "test",
                Value = new SaveAuction
                {
                    Bin = true,
                    StartingBid = 100,
                    Tag = "test",
                    End = DateTime.Now + TimeSpan.FromMinutes(5),
                    Start = DateTime.Now,
                    HighestBidAmount = 100,
                    Uuid = Guid.NewGuid().ToString(),
                    AuctioneerId = "384a029294fc445e863f2c42fe9709cb",
                    Enchantments = new() { new(Enchantment.EnchantmentType.sharpness, 7) },
                    FlatenedNBT = new(),
                    Bids = new() { new(){Amount=100, Bidder="384a029294fc445e863f2c42fe9709cb"} },
                }
            });
            Console.WriteLine($"Delivered '{result.Value}' to '{result.TopicPartitionOffset}'");

        }
    }
}
