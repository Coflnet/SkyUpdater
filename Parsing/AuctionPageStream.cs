using System;
using System.Buffers.Text;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Coflnet.Sky.Updater.Models;

namespace Coflnet.Sky.Updater;

/// <summary>
/// The identifying fields of an auction, read without deserializing the whole object
/// </summary>
public readonly struct AuctionHead
{
    public AuctionHead(Guid uuid, long startMs)
    {
        Uuid = uuid;
        StartMs = startMs;
    }

    public Guid Uuid { get; }
    public long StartMs { get; }
    public DateTime Start => new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc).AddMilliseconds(StartMs).ToLocalTime();
}

/// <summary>
/// Incrementally parses the /v2/skyblock/auctions payload with Utf8JsonReader.
/// Exposes lastUpdated as soon as its bytes arrived, then streams auctions
/// one by one while the body is still downloading.
/// </summary>
public class AuctionPageStream
{
    private readonly Stream stream;
    private byte[] buffer;
    private int filled;
    private int consumed;
    private bool finalBlock;
    private bool inArray;
    private bool lastUpdatedPropSeen;
    private JsonReaderState state = new();
    // the auction of the most recent head, valid until the enumeration advances or the buffer is refilled
    private bool currentValid;
    private int currentOffset;
    private int currentLength;
    private Auction currentAuction;

    public AuctionPageStream(Stream stream, int initialBufferSize = 64 * 1024)
    {
        this.stream = stream;
        buffer = new byte[initialBufferSize];
    }

    public async Task<long> ReadLastUpdated(CancellationToken token = default)
    {
        while (true)
        {
            if (TryReadLastUpdated(out var lastUpdated))
                return lastUpdated;
            if (finalBlock)
                throw new InvalidDataException("no lastUpdated field in response");
            await FillAsync(token).ConfigureAwait(false);
        }
    }

    public async IAsyncEnumerable<Auction> ReadAuctions([EnumeratorCancellation] CancellationToken token = default)
    {
        await foreach (var _ in ReadAuctionHeads(token).ConfigureAwait(false))
            yield return ReadCurrentAuction();
    }

    /// <summary>
    /// Yields uuid and start of every auction without deserializing it,
    /// <see cref="ReadCurrentAuction"/> deserializes the auction of the latest head.
    /// </summary>
    public async IAsyncEnumerable<AuctionHead> ReadAuctionHeads([EnumeratorCancellation] CancellationToken token = default)
    {
        while (true)
        {
            currentValid = false;
            AuctionHead head;
            bool endOfArray;
            while (!TryReadNextAuction(out head, out endOfArray))
            {
                if (finalBlock)
                    throw new InvalidDataException("response ended inside auctions array");
                await FillAsync(token).ConfigureAwait(false);
            }
            if (endOfArray)
                yield break;
            yield return head;
        }
    }

    /// <summary>
    /// Deserializes the auction the latest head belongs to. Valid until the enumeration advances.
    /// </summary>
    public Auction ReadCurrentAuction()
    {
        if (!currentValid)
            throw new InvalidOperationException("no current auction, read a head first and call this before advancing");
        return currentAuction ??= JsonSerializer.Deserialize<Auction>(buffer.AsSpan(currentOffset, currentLength));
    }

    internal static Guid ToId(string uuid)
    {
        if (Guid.TryParse(uuid, out var guid))
            return guid;
        return new Guid(MD5.HashData(Encoding.UTF8.GetBytes(uuid)));
    }

    private bool TryReadLastUpdated(out long lastUpdated)
    {
        lastUpdated = 0;
        var reader = new Utf8JsonReader(buffer.AsSpan(consumed, filled - consumed), finalBlock, state);
        while (reader.Read())
        {
            if (lastUpdatedPropSeen && reader.TokenType == JsonTokenType.Number)
            {
                lastUpdated = reader.GetInt64();
                Commit(ref reader);
                return true;
            }
            lastUpdatedPropSeen = reader.TokenType == JsonTokenType.PropertyName && reader.ValueTextEquals("lastUpdated");
        }
        Commit(ref reader);
        return false;
    }

    /// <summary>
    /// Attempts to read the next auction from buffered data.
    /// Returns false if more data is needed, sets endOfArray when the array closed.
    /// </summary>
    private bool TryReadNextAuction(out AuctionHead head, out bool endOfArray)
    {
        head = default;
        endOfArray = false;
        var reader = new Utf8JsonReader(buffer.AsSpan(consumed, filled - consumed), finalBlock, state);
        if (!inArray)
        {
            while (true)
            {
                if (!reader.Read())
                    return false; // need more data, nothing committed
                if (reader.TokenType == JsonTokenType.StartArray)
                {
                    inArray = true;
                    Commit(ref reader);
                    reader = new Utf8JsonReader(buffer.AsSpan(consumed, filled - consumed), finalBlock, state);
                    break;
                }
            }
        }
        if (!reader.Read())
            return false;
        if (reader.TokenType == JsonTokenType.EndArray)
        {
            endOfArray = true;
            Commit(ref reader);
            return true;
        }
        if (reader.TokenType != JsonTokenType.StartObject)
            throw new InvalidDataException("unexpected token in auctions array: " + reader.TokenType);
        // Probe a copy of the reader (it is a struct) so an incompletely buffered auction is
        // detected without a thrown exception. TrySkip returns false instead of throwing on a
        // non-final block, on the final block a truncated object still throws.
        var probe = reader;
        if (!probe.TrySkip())
            return false; // element only partially buffered, nothing consumed
        currentOffset = consumed + (int)reader.TokenStartIndex;
        currentLength = (int)(probe.BytesConsumed - reader.TokenStartIndex);
        currentAuction = null;
        Commit(ref probe);
        head = ReadHead(buffer.AsSpan(currentOffset, currentLength));
        currentValid = true;
        return true;
    }

    private AuctionHead ReadHead(ReadOnlySpan<byte> json)
    {
        var reader = new Utf8JsonReader(json, isFinalBlock: true, state: default);
        Guid uuid = default;
        long startMs = 0;
        bool hasUuid = false, hasStart = false;
        reader.Read();
        while (!(hasUuid && hasStart) && reader.Read() && reader.TokenType == JsonTokenType.PropertyName)
        {
            var isUuid = !hasUuid && reader.ValueTextEquals("uuid"u8);
            var isStart = !hasStart && reader.ValueTextEquals("start"u8);
            reader.Read();
            if (isUuid && reader.TokenType == JsonTokenType.String)
            {
                if (!reader.ValueIsEscaped && !reader.HasValueSequence
                    && Utf8Parser.TryParse(reader.ValueSpan, out Guid parsed, out int used, 'N') && used == reader.ValueSpan.Length)
                    uuid = parsed;
                else
                    uuid = ToId(reader.GetString());
                hasUuid = true;
            }
            else if (isStart && reader.TokenType == JsonTokenType.Number && reader.TryGetInt64(out startMs))
                hasStart = true;
            else
                reader.Skip();
        }
        if (hasUuid && hasStart)
            return new AuctionHead(uuid, startMs);
        // unusual object, the deserializer decides what it makes of it
        currentAuction = JsonSerializer.Deserialize<Auction>(json);
        return new AuctionHead(currentAuction.Uuid == null ? Guid.Empty : ToId(currentAuction.Uuid), currentAuction._start);
    }

    private void Commit(ref Utf8JsonReader reader)
    {
        consumed += (int)reader.BytesConsumed;
        state = reader.CurrentState;
    }

    private async Task FillAsync(CancellationToken token)
    {
        currentValid = false;
        if (consumed > 0)
        {
            Buffer.BlockCopy(buffer, consumed, buffer, 0, filled - consumed);
            filled -= consumed;
            consumed = 0;
        }
        else if (filled == buffer.Length)
        {
            var bigger = new byte[buffer.Length * 2];
            Buffer.BlockCopy(buffer, 0, bigger, 0, filled);
            buffer = bigger;
        }
        var read = await stream.ReadAsync(buffer.AsMemory(filled), token).ConfigureAwait(false);
        if (read == 0)
            finalBlock = true;
        filled += read;
    }
}
