using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;


namespace Coflnet.Sky.Updater
{
    public class Program
    {
        static string apiKey = SimplerConfig.Config.Instance["apiKey"];
        public static void Main(string[] args)
        {
            var bazzar = new BazaarUpdater();
            var updater = new Updater(apiKey);
            FileController.dataPaht = "/data";
            updater.UpdateForEver();
            bazzar.UpdateForEver(apiKey);

            CreateHostBuilder(args).Build().Run();
        }

        public static IHostBuilder CreateHostBuilder(string[] args) =>
            Host.CreateDefaultBuilder(args)
                .ConfigureWebHostDefaults(webBuilder =>
                {
                    webBuilder.UseStartup<Startup>();
                });
    }
}
