using System;
using NUnit.Framework;

namespace Coflnet.Sky.Updater.Tests;

public class SeenAuctionsTests
{
    private static Guid Id(int n) => new Guid(n, 0, 0, new byte[8]);

    [Test]
    public void OnlyTheFirstSightingCounts()
    {
        var seen = new SeenAuctions();
        var stamp = new DateTime(2026, 1, 1, 12, 0, 0, DateTimeKind.Utc);
        Assert.That(seen.FirstSighting(Id(1), stamp), Is.True);
        Assert.That(seen.FirstSighting(Id(1), stamp.AddMinutes(1)), Is.False);
        Assert.That(seen.FirstSighting(Id(2), stamp), Is.True);
    }

    [Test]
    public void ForgetDropsOldEntriesAndKeepsRecentOnes()
    {
        var seen = new SeenAuctions();
        var stamp = new DateTime(2026, 1, 1, 12, 0, 0, DateTimeKind.Utc);
        seen.FirstSighting(Id(3), stamp);
        seen.FirstSighting(Id(4), stamp.AddMinutes(5));

        seen.Forget(stamp.AddMinutes(2));

        Assert.That(seen.FirstSighting(Id(3), stamp.AddMinutes(5)), Is.True);
        Assert.That(seen.FirstSighting(Id(4), stamp.AddMinutes(5)), Is.False);
    }
}
