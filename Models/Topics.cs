using System.Text.Json.Serialization;

namespace Coflnet.Sky.Updater.Models;
public class Topics
{
    [JsonPropertyName("MISSING_AUCTION")]
    public string Missing_Auction { get; set; }
    [JsonPropertyName("SOLD_AUCTION")]
    public string Sold_Auction { get; set; }
    [JsonPropertyName("NEW_AUCTION")]
    public string New_Auction { get; set; }
    [JsonPropertyName("AUCTION_ENDED")]
    public string Auction_Ended { get; set; }
    [JsonPropertyName("NEW_BID")]
    public string New_Bid { get; set; }
    [JsonPropertyName("BAZAAR")]
    public string Bazaar { get; set; }
    [JsonPropertyName("AH_SUMARY")]
    public string Ah_Sumary { get; set; }
    [JsonPropertyName("AUCTION_CHECK")]
    public string Auction_Check { get; set; }
}