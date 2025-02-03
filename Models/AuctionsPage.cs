using System;
using System.Collections.Generic;
using System.Text.Json.Serialization;

namespace Coflnet.Sky.Updater.Models;
public class AuctionPage
{
    [JsonPropertyName("success")]
    public bool WasSuccessful { get; set; }

    [JsonPropertyName("cause")]
    public string Cause { get; set; }

    [JsonPropertyName("page")]
    public long Page { get; set; }

    [JsonPropertyName("totalPages")]
    public long TotalPages { get; set; }

    [JsonPropertyName("totalAuctions")]
    public long TotalAuctions { get; set; }

    [JsonPropertyName("lastUpdated")]
    public long _lastUpdated { get; set; }
    public DateTime LastUpdated
    {
        get
        {
            var convertToDateTime = new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc);
            convertToDateTime = convertToDateTime.AddMilliseconds(_lastUpdated).ToLocalTime();
            return convertToDateTime;
        }

    }

    [JsonPropertyName("auctions")]
    public List<Auction> Auctions { get; set; }
}
