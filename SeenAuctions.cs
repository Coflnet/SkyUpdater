using System;
using System.Collections.Concurrent;

namespace Coflnet.Sky.Updater;

/// <summary>
/// Remembers which auctions were already handed over, because the start time cutoff is wide enough to let
/// auctions of the previous snapshot through again
/// </summary>
internal class SeenAuctions
{
    private readonly ConcurrentDictionary<string, DateTime> seen = new();

    /// <summary>
    /// True the first time an uuid is seen, false afterwards
    /// </summary>
    public bool FirstSighting(string uuid, DateTime stamp) => seen.TryAdd(uuid, stamp);

    /// <summary>
    /// Drops everything recorded with a snapshot stamp before <paramref name="olderThan"/>
    /// </summary>
    public void Forget(DateTime olderThan)
    {
        foreach (var entry in seen)
        {
            if (entry.Value < olderThan)
                seen.TryRemove(entry.Key, out _);
        }
    }
}
