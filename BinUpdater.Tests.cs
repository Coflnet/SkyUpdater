using System;
using System.Collections.Concurrent;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using fNbt;
using fNbt.Tags;
using NUnit.Framework;

namespace Coflnet.Sky.Updater.Tests
{
    public class BinUpdaterTests
    {
        [TearDown]
        public void ResetState()
        {
            typeof(BinUpdater).GetField("LastFirst", BindingFlags.NonPublic | BindingFlags.Static).SetValue(null, null);
        }

        [Test]
        public void ClientIsReusedPerBaseUrl()
        {
            var first = BinUpdater.ClientFor("http://127.0.0.1:1");
            Assert.That(BinUpdater.ClientFor("http://127.0.0.1:1"), Is.SameAs(first));
            Assert.That(BinUpdater.ClientFor("http://127.0.0.1:2"), Is.Not.SameAs(first));
        }

        private static string ItemBytes()
        {
            var item = new NbtCompound("")
            {
                new NbtList("i", NbtTagType.Compound)
                {
                    new NbtCompound
                    {
                        new NbtShort("id", 276),
                        new NbtByte("Count", 1),
                        new NbtCompound("tag")
                        {
                            new NbtCompound("display") { new NbtString("Name", "\u00a7aTest item"), new NbtList("Lore", NbtTagType.String) { new NbtString("\u00a7a\u00a7lCOMMON") } },
                            new NbtCompound("ExtraAttributes") { new NbtString("id", "TEST_ITEM") }
                        }
                    }
                }
            };
            var file = new NbtFile(item);
            using var stream = new MemoryStream();
            file.SaveToStream(stream, NbtCompression.GZip);
            return Convert.ToBase64String(stream.ToArray());
        }

        [Test]
        public async Task SoldAuctionsShareOneConnection()
        {
            var port = FreePort();
            var prefix = $"http://127.0.0.1:{port}/";
            using var listener = new HttpListener();
            listener.Prefixes.Add(prefix);
            listener.Start();
            var remotePorts = new ConcurrentBag<int>();
            var requests = 0;
            var itemBytes = ItemBytes();
            var serve = Task.Run(async () =>
            {
                while (listener.IsListening)
                {
                    HttpListenerContext context;
                    try
                    {
                        context = await listener.GetContextAsync();
                    }
                    catch (Exception)
                    {
                        return;
                    }
                    remotePorts.Add(context.Request.RemoteEndPoint.Port);
                    var number = Interlocked.Increment(ref requests);
                    var uuid = number.ToString("x32");
                    var body = Encoding.UTF8.GetBytes("{\"success\":true,\"auctions\":[{\"auction_id\":\"" + uuid
                        + "\",\"seller\":\"" + new string('a', 32) + "\",\"seller_profile\":\"" + new string('b', 32)
                        + "\",\"buyer\":\"" + new string('c', 32) + "\",\"buyer_profile\":\"" + new string('d', 32)
                        + "\",\"timestamp\":1700000000000,\"price\":100,\"bin\":true,\"item_bytes\":\"" + itemBytes + "\"}]}");
                    context.Response.ContentType = "application/json";
                    context.Response.ContentLength64 = body.Length;
                    await context.Response.OutputStream.WriteAsync(body);
                    context.Response.Close();
                }
            });

            try
            {
                var first = await BinUpdater.DownloadSells(prefix.TrimEnd('/'));
                var second = await BinUpdater.DownloadSells(prefix.TrimEnd('/'));
                Assert.That(first, Has.Count.EqualTo(1));
                Assert.That(second, Has.Count.EqualTo(1));
                Assert.That(second[0].Uuid, Is.Not.EqualTo(first[0].Uuid));
            }
            finally
            {
                listener.Stop();
                await serve;
            }
            Assert.That(requests, Is.EqualTo(2));
            Assert.That(remotePorts.Distinct().Count(), Is.EqualTo(1), "every call opened its own connection");
        }

        private static int FreePort()
        {
            var probe = new TcpListener(IPAddress.Loopback, 0);
            probe.Start();
            var port = ((IPEndPoint)probe.LocalEndpoint).Port;
            probe.Stop();
            return port;
        }
    }
}
