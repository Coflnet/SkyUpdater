using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Coflnet.Sky.Updater.Models;
using NUnit.Framework;

namespace Coflnet.Sky.Updater.Tests
{
    public class AuctionPageStreamTests
    {
        private const string Body = "{\"success\":true,\"page\":0,\"totalPages\":1,\"totalAuctions\":4,\"lastUpdated\":1791849246275,\"auctions\":["
            + "{\"uuid\":\"aaaaaaaa11111111aaaaaaaa11111111\",\"auctioneer\":\"b1\",\"profile_id\":\"p1\",\"coop\":[\"b1\"],\"start\":1791849232660,\"end\":1792022032660,\"item_name\":\"Plain\",\"item_lore\":\"line\",\"extra\":\"x\",\"category\":\"misc\",\"tier\":\"COMMON\",\"starting_bid\":100,\"item_bytes\":\"H4sIAAAA\",\"claimed\":false,\"claimed_bidders\":[],\"highest_bid_amount\":0,\"bids\":[],\"bin\":true},"
            + "{\"uuid\":\"bbbbbbbb22222222bbbbbbbb22222222\",\"auctioneer\":\"b2\",\"profile_id\":\"p2\",\"coop\":[\"b2\"],\"start\":1791849232661,\"end\":1792022032661,\"item_name\":\"Bids\",\"item_lore\":\"l\",\"extra\":\"x\",\"category\":\"weapon\",\"tier\":\"EPIC\",\"starting_bid\":5000,\"item_bytes\":\"H4sIBBBB\",\"claimed\":false,\"claimed_bidders\":[],\"highest_bid_amount\":7000,\"bids\":[{\"auction_id\":\"bbbbbbbb22222222bbbbbbbb22222222\",\"bidder\":\"c1\",\"profile_id\":\"cp1\",\"amount\":6000,\"timestamp\":1791849240000},{\"auction_id\":\"bbbbbbbb22222222bbbbbbbb22222222\",\"bidder\":\"c2\",\"profile_id\":\"cp2\",\"amount\":7000,\"timestamp\":1791849250000}],\"bin\":false},"
            + "{\"uuid\":\"cccccccc33333333cccccccc33333333\",\"auctioneer\":\"b3\",\"profile_id\":\"p3\",\"coop\":[\"b3\"],\"start\":1791849232662,\"end\":1792022032662,\"item_name\":\"Quotes\",\"item_lore\":\"say \\\"hi\\\" {not} [an] object }{ \\\\ end\",\"extra\":\"x\",\"category\":\"misc\",\"tier\":\"RARE\",\"starting_bid\":300,\"item_bytes\":\"H4sICCCC\",\"claimed\":false,\"claimed_bidders\":[],\"highest_bid_amount\":0,\"bids\":[],\"bin\":true},"
            + "{\"uuid\":\"dddddddd44444444dddddddd44444444\",\"auctioneer\":\"b4\",\"profile_id\":\"p4\",\"coop\":[\"b4\"],\"start\":1791849232663,\"end\":1792022032663,\"item_name\":\"\\u00dcber \\u2605 äö 😀\",\"item_lore\":\"§6Unicode ❤\",\"extra\":\"x\",\"category\":\"misc\",\"tier\":\"LEGENDARY\",\"starting_bid\":400,\"item_bytes\":\"H4sIDDDD\",\"claimed\":false,\"claimed_bidders\":[],\"highest_bid_amount\":0,\"bids\":[],\"bin\":true}"
            + "]}";

        private static int jsonExceptions;

        private static byte[] BodyBytes => Encoding.UTF8.GetBytes(Body);

        private static async Task<List<Auction>> ReadAll(byte[] body, int chunk)
        {
            var parser = new AuctionPageStream(new ChunkedStream(body, chunk));
            await parser.ReadLastUpdated();
            var result = new List<Auction>();
            await foreach (var a in parser.ReadAuctions())
                result.Add(a);
            return result;
        }

        private static string Describe(Auction a)
            => $"{a.Uuid}|{a._start}|{a.BuyItNow}|{a.ItemBytes}|{a.StartingBid}|{a.Bids?.Count ?? -1}";

        /// <summary>
        /// An auction that was not completely buffered used to be detected by a thrown JsonException
        /// on the latency critical reader thread. Chunked delivery must give the same auctions
        /// without raising any first-chance JsonException.
        /// </summary>
        [Test]
        public async Task SmallChunksYieldSameAuctionsWithoutExceptions()
        {
            var body = BodyBytes;
            var whole = await ReadAll(body, body.Length);

            EventHandler<System.Runtime.ExceptionServices.FirstChanceExceptionEventArgs> handler = (s, e) =>
            {
                if (e.Exception is JsonException)
                    Interlocked.Increment(ref jsonExceptions);
            };
            jsonExceptions = 0;
            List<Auction> chunked;
            AppDomain.CurrentDomain.FirstChanceException += handler;
            try
            {
                chunked = await ReadAll(body, 97);
            }
            finally
            {
                AppDomain.CurrentDomain.FirstChanceException -= handler;
            }

            Assert.That(whole.Count, Is.EqualTo(4));
            Assert.That(chunked.Select(Describe), Is.EqualTo(whole.Select(Describe)));
            Assert.That(chunked[1].Bids.Count, Is.EqualTo(2));
            Assert.That(chunked[2].ItemLore, Does.Contain("{not}"));
            Assert.That(jsonExceptions, Is.EqualTo(0));
        }

        [Test]
        public async Task SingleByteChunksYieldSameAuctions()
        {
            var body = BodyBytes;
            var whole = await ReadAll(body, body.Length);
            var chunked = await ReadAll(body, 1);
            Assert.That(chunked.Select(Describe), Is.EqualTo(whole.Select(Describe)));
        }

        [Test]
        public void TruncatedBodyStillThrows()
        {
            var body = BodyBytes;
            var cut = Body.IndexOf("\"item_name\":\"Bids\"", StringComparison.Ordinal) + 20;
            var truncated = body.AsSpan(0, cut).ToArray();
            Assert.CatchAsync<JsonException>(async () => await ReadAll(truncated, 97));
            Assert.CatchAsync<JsonException>(async () => await ReadAll(truncated, truncated.Length));
        }

        [Test]
        public async Task ReadLastUpdatedWhenFirstChunkEndsBeforeNumber()
        {
            var body = BodyBytes;
            var cut = Body.IndexOf("lastUpdated", StringComparison.Ordinal) + "lastUpdated\":".Length;
            var parser = new AuctionPageStream(new ChunkedStream(body, cut));
            Assert.That(await parser.ReadLastUpdated(), Is.EqualTo(1791849246275));
            var count = 0;
            await foreach (var _ in parser.ReadAuctions())
                count++;
            Assert.That(count, Is.EqualTo(4));
        }
    }
}
