using System;
using System.Collections.Generic;
using System.Text.Json.Serialization;

namespace Coflnet.Sky.Updater.Models
{
    public class Auction
    {
        [JsonPropertyName("uuid")]
        public string Uuid { get; set; }

        [JsonPropertyName("auctioneer")]
        public string Auctioneer { get; set; }
        [JsonPropertyName("profile_id")]
        [Newtonsoft.Json.JsonProperty("profile_id")]
        public string ProfileId { get; set; }

        [JsonPropertyName("start")]
        [Newtonsoft.Json.JsonProperty("start")]
        public long _start { get; set; }
        [JsonIgnore]
        [Newtonsoft.Json.JsonIgnore]
        public DateTime Start
        {
            get
            {
                var convertToDateTime = new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc);
                convertToDateTime = convertToDateTime.AddMilliseconds(_start).ToLocalTime();
                return convertToDateTime;
            }
        }

        [JsonPropertyName("end")]
        [Newtonsoft.Json.JsonProperty("end")]
        public long _end { get; set; }
        [JsonIgnore]
        [Newtonsoft.Json.JsonIgnore]
        public DateTime End
        {
            get
            {
                var convertToDateTime = new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc);
                convertToDateTime = convertToDateTime.AddMilliseconds(_end).ToLocalTime();
                return convertToDateTime;
            }
        }

        [JsonPropertyName("item_name")]
        [Newtonsoft.Json.JsonProperty("item_name")]
        public string ItemName { get; set; }


        [JsonPropertyName("item_lore")]
        [Newtonsoft.Json.JsonProperty("item_lore")]
        public string ItemLore { get; set; }

        [JsonPropertyName("extra")]
        [Newtonsoft.Json.JsonProperty("extra")]
        public string Extra { get; set; }

        [JsonPropertyName("category")]
        [Newtonsoft.Json.JsonProperty("category")]
        public string Category { get; set; }

        [JsonPropertyName("claimed")]
        public bool Claimed { get; set; }

        [JsonPropertyName("claimed_bidders")]
        [Newtonsoft.Json.JsonProperty("claimed_bidders")]
        public List<string> ClaimedBidders { get; set; }
        [JsonPropertyName("coop")]
        public List<string> Coop { get; set; }

        [JsonPropertyName("tier")]
        [Newtonsoft.Json.JsonProperty("tier")]
        public string Tier { get; set; }

        [JsonPropertyName("starting_bid")]
        [Newtonsoft.Json.JsonProperty("starting_bid")]
        public long StartingBid { get; set; }

        [JsonPropertyName("item_bytes")]
        [Newtonsoft.Json.JsonProperty("item_bytes")]
        public string ItemBytes { get; set; }

        [JsonPropertyName("highest_bid_amount")]
        [Newtonsoft.Json.JsonProperty("highest_bid_amount")]
        public long HighestBidAmount { get; set; }
        [JsonPropertyName("bids")]
        [Newtonsoft.Json.JsonProperty("bids")]
        public List<Bids> Bids { get; set; }

        [JsonPropertyName("bin")]
        [Newtonsoft.Json.JsonProperty("bin")]
        public bool BuyItNow { get; set; }
    }
}