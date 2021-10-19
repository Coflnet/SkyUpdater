using System;
using System.Text.Json.Serialization;

namespace Coflnet.Sky.Updater.Models
{
    public class Bids
    {
        [JsonPropertyName("auction_id")]
        [Newtonsoft.Json.JsonProperty("auction_id")]
        public string AuctionId { get; set; }

        [JsonPropertyName("bidder")]
        public string Bidder { get; set; }

        [JsonPropertyName("profile_id")]
        [Newtonsoft.Json.JsonProperty("profile_id")]
        public string ProfileId { get; set; }

        [JsonPropertyName("amount")]
        public long Amount { get; set; }

        [JsonPropertyName("timestamp")]
        [Newtonsoft.Json.JsonProperty("timestamp")]
        public long _timestamp { get; set; }
        [Newtonsoft.Json.JsonIgnore]
        public DateTime Timestamp
        {
            get
            {
                var convertToDateTime = new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc);
                convertToDateTime = convertToDateTime.AddMilliseconds(_timestamp).ToLocalTime();
                return convertToDateTime;
            }

        }
    }
}