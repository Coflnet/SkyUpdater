using System.Text.Json.Serialization;

namespace Coflnet.Sky.Updater.Models;
public class Topics
{
    [JsonPropertyName("MISSING_AUCTION")]
    public string MissingAuction { get; set; }
    [JsonPropertyName("SOLD_AUCTION")]
    public string SoldAuction { get; set; }
    [JsonPropertyName("NEW_AUCTION")]
    public string NewAuction { get; set; }
    [JsonPropertyName("AUCTION_ENDED")]
    public string AuctionEnded { get; set; }
    [JsonPropertyName("NEW_BID")]
    public string NewBid { get; set; }
    [JsonPropertyName("BAZAAR")]
    public string Bazaar { get; set; }
    [JsonPropertyName("AH_SUMARY")]
    public string AhSumary { get; set; }
    [JsonPropertyName("AUCTION_CHECK")]
    public string AuctionCheck { get; set; }
}