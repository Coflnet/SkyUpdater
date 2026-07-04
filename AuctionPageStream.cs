using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Coflnet.Sky.Updater.Models;

namespace Coflnet.Sky.Updater;

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
        while (true)
        {
            Auction auction;
            bool endOfArray;
            while (!TryReadNextAuction(out auction, out endOfArray))
            {
                if (finalBlock)
                    throw new InvalidDataException("response ended inside auctions array");
                await FillAsync(token).ConfigureAwait(false);
            }
            if (endOfArray)
                yield break;
            yield return auction;
        }
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
    private bool TryReadNextAuction(out Auction auction, out bool endOfArray)
    {
        auction = null;
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
        try
        {
            auction = JsonSerializer.Deserialize<Auction>(ref reader);
        }
        catch (JsonException)
        {
            if (finalBlock)
                throw;
            return false; // element only partially buffered, retry after fill
        }
        Commit(ref reader);
        return true;
    }

    private void Commit(ref Utf8JsonReader reader)
    {
        consumed += (int)reader.BytesConsumed;
        state = reader.CurrentState;
    }

    private async Task FillAsync(CancellationToken token)
    {
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
