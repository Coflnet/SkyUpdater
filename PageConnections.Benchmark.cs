using System;
using System.Collections.Generic;
using System.Diagnostics.Tracing;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using NUnit.Framework;

namespace Coflnet.Sky.Updater.Tests
{
    /// <summary>
    /// Live check against the real api that three concurrent downloads share one connection with one client
    /// and open one each with three clients. Six requests in total.
    /// </summary>
    public class PageConnectionsBenchmark
    {
        private class ConnectionCounter : EventListener
        {
            public int Established;

            protected override void OnEventSourceCreated(EventSource eventSource)
            {
                if (eventSource.Name == "System.Net.Http")
                    EnableEvents(eventSource, EventLevel.Informational, EventKeywords.All);
            }

            protected override void OnEventWritten(EventWrittenEventArgs e)
            {
                if (e.EventName == "ConnectionEstablished")
                    System.Threading.Interlocked.Increment(ref Established);
            }
        }

        private static readonly string[] Paths = { "/v2/skyblock/auctions", "/v2/skyblock/auctions?page=0", "/v2/skyblock/auctions?page=0&t=x" };

        private static async Task<HttpResponseMessage> Send(HttpClient client, string path)
        {
            var message = new HttpRequestMessage(HttpMethod.Get, "https://api.hypixel.net" + path)
            {
                Version = System.Net.HttpVersion.Version20,
                VersionPolicy = HttpVersionPolicy.RequestVersionOrLower
            };
            message.Headers.From = "Coflnet";
            return await client.SendAsync(message, HttpCompletionOption.ResponseHeadersRead).ConfigureAwait(false);
        }

        private static async Task Run(string name, Func<int, HttpClient> clientFor)
        {
            using var counter = new ConnectionCounter();
            var responses = await Task.WhenAll(Paths.Select((path, i) => Send(clientFor(i), path)));
            // all three stay open at the same time until here
            var versions = string.Join(", ", responses.Select(r => r.Version));
            foreach (var response in responses)
                response.Dispose();
            // events arrive on their own thread
            await Task.Delay(200);
            Console.WriteLine($"{name}: connections {counter.Established} versions {versions}");
        }

        [Test]
        [Explicit]
        [Category("Benchmark")]
        public async Task CountConnectionsOfSharedAndSeparateClients()
        {
            using var one = NewUpdater.CreatePageClient();
            await Run("one client", _ => one);

            var three = Enumerable.Range(0, 3).Select(_ => NewUpdater.CreatePageClient()).ToList();
            await Run("three clients", i => three[i]);
            three.ForEach(c => c.Dispose());
        }
    }
}
