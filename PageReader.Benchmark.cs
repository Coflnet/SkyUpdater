using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;
using Coflnet.Sky.Core;
using Coflnet.Sky.Updater.Models;
using NUnit.Framework;

namespace Coflnet.Sky.Updater.Tests
{
    [JsonSerializable(typeof(Auction))]
    internal partial class BenchAuctionContext : JsonSerializerContext
    {
    }

    /// <summary>
    /// Stream that hands out at most maxChunk bytes per read and counts the reads.
    /// </summary>
    public class ChunkedStream : Stream
    {
        private readonly byte[] data;
        private readonly int maxChunk;
        private int position;
        public int ReadCalls;

        public ChunkedStream(byte[] data, int maxChunk)
        {
            this.data = data;
            this.maxChunk = maxChunk;
        }

        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => data.Length;
        public override long Position { get => position; set => throw new NotSupportedException(); }
        public override void Flush() { }
        public override int Read(byte[] buffer, int offset, int count) => Read(buffer.AsSpan(offset, count));
        public override int Read(Span<byte> buffer)
        {
            ReadCalls++;
            var n = Math.Min(Math.Min(buffer.Length, maxChunk), data.Length - position);
            data.AsSpan(position, n).CopyTo(buffer);
            position += n;
            return n;
        }
        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
            => new ValueTask<int>(Read(buffer.Span));
        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
            => Task.FromResult(Read(buffer.AsSpan(offset, count)));
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }

    [Explicit]
    [Category("Benchmark")]
    public class PageReaderBenchmark
    {
        private const int AuctionCount = 250;

        [Test]
        public async Task Run()
        {
            var path = Environment.GetEnvironmentVariable("AUCTION_PAGE_FIXTURE");
            if (string.IsNullOrEmpty(path))
                Assert.Ignore("AUCTION_PAGE_FIXTURE not set");
            var body = File.ReadAllBytes(path);
            Out($"fixture {body.Length} bytes");
            await PartA(body);
            PartB(body);
        }

        private static void Out(string s)
        {
            Console.WriteLine(s);
        }

        private static int jsonExceptions;

        private static async Task<(double micros, int reads, int exceptions)> RunOnce(byte[] body, int chunk)
        {
            var stream = new ChunkedStream(body, chunk);
            var parser = new AuctionPageStream(stream);
            jsonExceptions = 0;
            var sw = Stopwatch.StartNew();
            await parser.ReadLastUpdated();
            var count = 0;
            await foreach (var a in parser.ReadAuctions())
            {
                if (++count >= AuctionCount)
                    break;
            }
            sw.Stop();
            return (sw.Elapsed.TotalMilliseconds * 1000, stream.ReadCalls, jsonExceptions);
        }

        private async Task PartA(byte[] body)
        {
            EventHandler<System.Runtime.ExceptionServices.FirstChanceExceptionEventArgs> handler = (s, e) =>
            {
                if (e.Exception is JsonException)
                    Interlocked.Increment(ref jsonExceptions);
            };
            AppDomain.CurrentDomain.FirstChanceException += handler;
            try
            {
                Out("== A: AuctionPageStream, first 250 auctions (median of 40 runs after 20 warm-ups)");
                foreach (var chunk in new[] { 1400, 8192, 65536 })
                {
                    for (int i = 0; i < 20; i++)
                        await RunOnce(body, chunk);
                    var runs = new List<(double micros, int reads, int exceptions)>();
                    for (int i = 0; i < 40; i++)
                        runs.Add(await RunOnce(body, chunk));
                    var sorted = runs.OrderBy(r => r.micros).ToList();
                    var median = sorted[sorted.Count / 2];
                    Out($"chunk {chunk,6}: total {median.micros,9:F0} us  per auction {median.micros / AuctionCount,7:F1} us  ReadAsync calls {median.reads,4}  JsonExceptions {median.exceptions,4}");
                }
            }
            finally
            {
                AppDomain.CurrentDomain.FirstChanceException -= handler;
            }
        }

        private static List<byte[]> SplitAuctions(byte[] body)
        {
            var result = new List<byte[]>();
            var reader = new Utf8JsonReader(body);
            while (reader.Read() && reader.TokenType != JsonTokenType.StartArray && reader.CurrentDepth < 2) { }
            // the first StartArray at depth 1 is the auctions array ("auctions" is the only top level array)
            while (reader.TokenType != JsonTokenType.StartArray && reader.Read()) { }
            while (reader.Read() && reader.TokenType == JsonTokenType.StartObject && result.Count < AuctionCount)
            {
                var start = (int)reader.TokenStartIndex;
                reader.TrySkip();
                var end = (int)reader.BytesConsumed;
                result.Add(body.AsSpan(start, end - start).ToArray());
            }
            return result;
        }

        private class Stat
        {
            public List<double> Micros = new();
            public List<long> Bytes = new();
            public void Add(long ticks, long bytes)
            {
                Micros.Add(ticks * 1_000_000.0 / Stopwatch.Frequency);
                Bytes.Add(bytes);
            }
            public string Summary()
            {
                var m = Micros.OrderBy(x => x).ToList();
                var b = Bytes.OrderBy(x => x).ToList();
                return $"time us median {m[m.Count / 2],7:F1} p90 {m[(int)(m.Count * 0.9)],7:F1} max {m[^1],8:F1} | alloc B median {b[b.Count / 2],7} p90 {b[(int)(b.Count * 0.9)],7} max {b[^1],7}";
            }
        }

        private static Stat Measure(int count, int warmups, int runs, Action<int> action)
        {
            for (int w = 0; w < warmups; w++)
                for (int i = 0; i < count; i++)
                    action(i);
            var stat = new Stat();
            for (int r = 0; r < runs; r++)
                for (int i = 0; i < count; i++)
                {
                    var before = GC.GetAllocatedBytesForCurrentThread();
                    var t = Stopwatch.GetTimestamp();
                    action(i);
                    var el = Stopwatch.GetTimestamp() - t;
                    stat.Add(el, GC.GetAllocatedBytesForCurrentThread() - before);
                }
            return stat;
        }

        private static double MedianPerIndex(Stat s, int count, int runs, int index)
        {
            var list = new List<double>();
            for (int r = 0; r < runs; r++)
                list.Add(s.Micros[r * count + index]);
            list.Sort();
            return list[list.Count / 2];
        }

        private static void ReadHeadsOnly(byte[] page)
        {
            var parser = new AuctionPageStream(new MemoryStream(page));
            parser.ReadLastUpdated().GetAwaiter().GetResult();
            var e = parser.ReadAuctionHeads().GetAsyncEnumerator();
            while (e.MoveNextAsync().AsTask().GetAwaiter().GetResult()) { }
        }

        private void PartB(byte[] body)
        {
            var raw = SplitAuctions(body);
            Out($"== B: per auction on one thread ({raw.Count} auctions, 20 warm-up passes, 30 timed passes pooled)");
            const int runs = 30;
            var parsed = raw.Select(r => JsonSerializer.Deserialize<Auction>(r)).ToList();

            var full = Measure(raw.Count, 20, runs, i => JsonSerializer.Deserialize<Auction>(raw[i]));
            Out("(i)   reflection Deserialize<Auction>: " + full.Summary());

            var page = Encoding.UTF8.GetBytes("{\"lastUpdated\":1,\"auctions\":[")
                .Concat(raw.Select((r, i) => i == 0 ? r : new byte[] { (byte)',' }.Concat(r).ToArray()).SelectMany(r => r))
                .Concat(Encoding.UTF8.GetBytes("]}")).ToArray();
            var heads = new Stat();
            for (int pass = 0; pass < 20 + runs; pass++)
            {
                var before = GC.GetAllocatedBytesForCurrentThread();
                var t = Stopwatch.GetTimestamp();
                ReadHeadsOnly(page);
                var el = Stopwatch.GetTimestamp() - t;
                if (pass >= 20)
                    heads.Add(el / raw.Count, (GC.GetAllocatedBytesForCurrentThread() - before) / raw.Count);
            }
            Out("(i-c) heads only, per auction over a whole page pass (includes read buffer): " + heads.Summary());

            var gen = Measure(raw.Count, 20, runs, i => JsonSerializer.Deserialize(raw[i], BenchAuctionContext.Default.Auction));
            Out("(i-b) source-generated Deserialize    : " + gen.Summary());

            var probe = Measure(raw.Count, 20, runs, i =>
            {
                var reader = new Utf8JsonReader(raw[i]);
                reader.Read();
                reader.TrySkip();
            });
            Out("(ii)  token walk (TrySkip)            : " + probe.Summary());

            var binIdx = Enumerable.Range(0, parsed.Count).Where(i => parsed[i].BuyItNow).ToList();
            Out($"(iii) ConvertAuction for {binIdx.Count} BIN auctions");
            var lastUpdated = DateTime.UtcNow.AddSeconds(-3);
            var findTime = DateTime.UtcNow;
            var convert = Measure(binIdx.Count, 20, runs, i => Updater.ConvertAuction(parsed[binIdx[i]], lastUpdated, findTime));
            Out("      ConvertAuction                  : " + convert.Summary());

            var fill = Measure(binIdx.Count, 20, runs, i => NBT.FillDetails(new SaveAuction(), parsed[binIdx[i]].ItemBytes));
            Out("      NBT.FillDetails (fresh SaveAuction): " + fill.Summary());
            var fm = fill.Micros.OrderBy(x => x).ToList();
            var cm = convert.Micros.OrderBy(x => x).ToList();
            Out($"      split by median: FillDetails {fm[fm.Count / 2]:F1} us of ConvertAuction {cm[cm.Count / 2]:F1} us, rest {cm[cm.Count / 2] - fm[fm.Count / 2]:F1} us");

            var slow = Enumerable.Range(0, binIdx.Count)
                .Select(i => (name: parsed[binIdx[i]].ItemName, us: MedianPerIndex(convert, binIdx.Count, runs, i)))
                .OrderByDescending(x => x.us).Take(5);
            Out("      five slowest (median per auction over passes):");
            foreach (var s in slow)
                Out($"        {s.us,8:F1} us  {s.name}");
        }
    }
}
