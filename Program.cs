using Coflnet.Security.OpenBao;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Hosting;


namespace Coflnet.Sky.Updater;

public class Program
{
    //static string apiKey = SimplerConfig.Config.Instance["apiKey"];
    public static void Main(string[] args)
    {
        CreateHostBuilder(args).Build().Run();
    }

    public static IHostBuilder CreateHostBuilder(string[] args) =>
        Host.CreateDefaultBuilder(args)
                .ConfigureAppConfiguration((_, config) => config.AddOpenBaoFromEnvironment())
            .ConfigureWebHostDefaults(webBuilder =>
            {
                webBuilder.UseStartup<Startup>();
            });
}
