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

        private static string Page(params string[] auctions)
            => "{\"success\":true,\"lastUpdated\":1791849246275,\"auctions\":[" + string.Join(",", auctions) + "]}";

        private static async Task<List<(AuctionHead head, Auction auction)>> ReadHeads(byte[] body, int chunk, Func<int, bool> materialize = null)
        {
            var parser = new AuctionPageStream(new ChunkedStream(body, chunk));
            await parser.ReadLastUpdated();
            var result = new List<(AuctionHead, Auction)>();
            var i = 0;
            await foreach (var head in parser.ReadAuctionHeads())
                result.Add((head, materialize == null || materialize(i++) ? parser.ReadCurrentAuction() : null));
            return result;
        }

        private static List<Auction> ExpectedAuctions(string json)
        {
            using var doc = JsonDocument.Parse(json);
            return doc.RootElement.GetProperty("auctions").EnumerateArray()
                .Select(e => JsonSerializer.Deserialize<Auction>(e.GetRawText())).ToList();
        }

        [TestCase(1)]
        [TestCase(7)]
        [TestCase(64)]
        [TestCase(1400)]
        [TestCase(65536)]
        public async Task HeadsWithCurrentAuctionEqualPlainDeserialize(int chunk)
        {
            var expected = ExpectedAuctions(Body);
            var read = await ReadHeads(BodyBytes, chunk);
            Assert.That(read.Select(r => JsonSerializer.Serialize(r.auction)), Is.EqualTo(expected.Select(e => JsonSerializer.Serialize(e))));
            Assert.That(read.Select(r => r.head.StartMs), Is.EqualTo(expected.Select(e => e._start)));
            Assert.That(read.Select(r => r.head.Uuid), Is.EqualTo(expected.Select(e => AuctionPageStream.ToId(e.Uuid))));
        }

        [Test]
        public async Task OnlySomeAuctionsMaterialized()
        {
            var expected = ExpectedAuctions(Body);
            var read = await ReadHeads(BodyBytes, 7, i => i % 3 == 0);
            Assert.That(read.Count, Is.EqualTo(4));
            Assert.That(read.Select(r => r.auction != null), Is.EqualTo(new[] { true, false, false, true }));
            Assert.That(JsonSerializer.Serialize(read[3].auction), Is.EqualTo(JsonSerializer.Serialize(expected[3])));
            Assert.That(read.Select(r => r.head.StartMs), Is.EqualTo(expected.Select(e => e._start)));
            Assert.That(read.Select(r => r.head.Uuid), Is.EqualTo(expected.Select(e => AuctionPageStream.ToId(e.Uuid))));
        }

        [Test]
        public async Task HeadFieldsAfterOtherPropertiesAndNestedValues()
        {
            var json = Page("{\"coop\":[\"x\"],\"other\":{\"start\":5,\"uuid\":[1],\"deep\":{\"uuid\":\"nested\"}},\"item_name\":\"n\",\"start\":1791849232660,\"uuid\":\"aaaaaaaa11111111aaaaaaaa11111111\",\"end\":3}");
            foreach (var chunk in new[] { 1, 5, 4096 })
            {
                var read = await ReadHeads(Encoding.UTF8.GetBytes(json), chunk);
                Assert.That(read.Count, Is.EqualTo(1));
                Assert.That(read[0].head.StartMs, Is.EqualTo(1791849232660));
                Assert.That(read[0].head.Uuid, Is.EqualTo(Guid.Parse("aaaaaaaa11111111aaaaaaaa11111111")));
                Assert.That(read[0].auction.ItemName, Is.EqualTo("n"));
            }
        }

        [Test]
        public async Task MissingOrMalformedHeadFieldsFallBackToDeserialize()
        {
            var json = Page(
                "{\"uuid\":\"aaaaaaaa11111111aaaaaaaa11111111\",\"item_name\":\"no start\"}",
                "{\"start\":42,\"item_name\":\"no uuid\"}",
                "{\"uuid\":\"aaaaaaaa-1111-1111-aaaa-aaaa11111111\",\"start\":43,\"item_name\":\"dashed\"}");
            var read = await ReadHeads(Encoding.UTF8.GetBytes(json), 3);
            Assert.That(read.Select(r => r.auction.ItemName), Is.EqualTo(new[] { "no start", "no uuid", "dashed" }));
            Assert.That(read[0].head.StartMs, Is.EqualTo(0));
            Assert.That(read[1].head.Uuid, Is.EqualTo(Guid.Empty));
            Assert.That(read[1].head.StartMs, Is.EqualTo(42));
            Assert.That(read[2].head.Uuid, Is.EqualTo(Guid.Parse("aaaaaaaa11111111aaaaaaaa11111111")));
        }

        [Test]
        public void NonStringUuidIsLeftToTheDeserializer()
        {
            var json = Encoding.UTF8.GetBytes(Page("{\"uuid\":{\"a\":1},\"start\":44}"));
            Assert.CatchAsync<JsonException>(async () => await ReadHeads(json, 3));
        }

        [Test]
        public void ToIdIsStable()
        {
            Assert.That(AuctionPageStream.ToId("not-a-guid"), Is.EqualTo(AuctionPageStream.ToId("not-a-guid")));
            Assert.That(AuctionPageStream.ToId("not-a-guid"), Is.Not.EqualTo(AuctionPageStream.ToId("not-a-guid2")));
            Assert.That(AuctionPageStream.ToId("aaaaaaaa11111111aaaaaaaa11111111"), Is.EqualTo(AuctionPageStream.ToId("aaaaaaaa-1111-1111-aaaa-aaaa11111111")));
        }

        [Test]
        public async Task NonHexIdGivesStableHead()
        {
            var json = Page("{\"uuid\":\"zzzz\",\"start\":1}", "{\"uuid\":\"zzzz\",\"start\":2}");
            var read = await ReadHeads(Encoding.UTF8.GetBytes(json), 4);
            Assert.That(read[0].head.Uuid, Is.EqualTo(read[1].head.Uuid));
            Assert.That(read[0].head.Uuid, Is.EqualTo(AuctionPageStream.ToId("zzzz")));
        }

        [Test]
        public async Task EscapedStringValuesSurvive()
        {
            var name = "\u00a7aName \\\"q\\\" back\\\\slash";
            var json = Page("{\"item_name\":\"" + name + "\",\"item_lore\":\"\u00a77l1\\n\\\"x\\\" \\\\ \u00a7c\",\"start\":1,\"uuid\":\"aaaaaaaa11111111aaaaaaaa11111111\"}");
            var expected = ExpectedAuctions(json);
            var read = await ReadHeads(Encoding.UTF8.GetBytes(json), 5);
            Assert.That(read[0].auction.ItemName, Is.EqualTo(expected[0].ItemName));
            Assert.That(read[0].auction.ItemLore, Is.EqualTo(expected[0].ItemLore));
            Assert.That(read[0].auction.ItemName, Does.Contain("\u00a7a").And.Contain("\"q\"").And.Contain("back\\slash"));
        }

        [Test]
        public async Task EscapedUuidStillParses()
        {
            var json = Page("{\"uuid\":\"\\u0061aaaaaaa11111111aaaaaaaa11111111\",\"start\":1}");
            var read = await ReadHeads(Encoding.UTF8.GetBytes(json), 6);
            Assert.That(read[0].head.Uuid, Is.EqualTo(Guid.Parse("aaaaaaaa11111111aaaaaaaa11111111")));
        }

        [Test]
        public async Task ReadCurrentAuctionOnlyValidForCurrentHead()
        {
            var parser = new AuctionPageStream(new ChunkedStream(BodyBytes, 50));
            await parser.ReadLastUpdated();
            Assert.Throws<InvalidOperationException>(() => parser.ReadCurrentAuction());
            await using var e = parser.ReadAuctionHeads().GetAsyncEnumerator();
            Assert.That(await e.MoveNextAsync(), Is.True);
            var first = parser.ReadCurrentAuction();
            Assert.That(parser.ReadCurrentAuction(), Is.SameAs(first));
            Assert.That(await e.MoveNextAsync(), Is.True);
            Assert.That(parser.ReadCurrentAuction(), Is.Not.SameAs(first));
            Assert.That(parser.ReadCurrentAuction().ItemName, Is.EqualTo("Bids"));
            Assert.That(await e.MoveNextAsync(), Is.True);
            Assert.That(await e.MoveNextAsync(), Is.True);
            Assert.That(await e.MoveNextAsync(), Is.False);
            Assert.Throws<InvalidOperationException>(() => parser.ReadCurrentAuction());
        }
    }
}
