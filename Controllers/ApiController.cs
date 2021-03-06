using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Coflnet.Sky.Updater;
using Coflnet.Sky.Updater.Models;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;

namespace SkyUpdater.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class ApiController : ControllerBase
    {
        private readonly ILogger<ApiController> _logger;

        public ApiController(ILogger<ApiController> logger)
        {
            _logger = logger;
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
    }
}
