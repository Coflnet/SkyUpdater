using System;
using NUnit.Framework;

namespace Coflnet.Sky.Updater.Tests;

public class SeenAuctionsTests
{
    [Test]
    public void OnlyTheFirstSightingCounts()
    {
        var seen = new SeenAuctions();
        var stamp = new DateTime(2026, 1, 1, 12, 0, 0, DateTimeKind.Utc);
        Assert.That(seen.FirstSighting("a", stamp), Is.True);
        Assert.That(seen.FirstSighting("a", stamp.AddMinutes(1)), Is.False);
        Assert.That(seen.FirstSighting("b", stamp), Is.True);
    }

    [Test]
    public void ForgetDropsOldEntriesAndKeepsRecentOnes()
    {
        var seen = new SeenAuctions();
        var stamp = new DateTime(2026, 1, 1, 12, 0, 0, DateTimeKind.Utc);
        seen.FirstSighting("old", stamp);
        seen.FirstSighting("recent", stamp.AddMinutes(5));

        seen.Forget(stamp.AddMinutes(2));

        Assert.That(seen.FirstSighting("old", stamp.AddMinutes(5)), Is.True);
        Assert.That(seen.FirstSighting("recent", stamp.AddMinutes(5)), Is.False);
    }
}
