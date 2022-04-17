using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Coflnet.Sky.Updater;
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
            return Updater.LastPullComplete;
        }
    }
}
