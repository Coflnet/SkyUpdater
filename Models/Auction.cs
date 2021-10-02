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

        [JsonPropertyName("start")]
        public long _start { get; set; }
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
        public long _end { get; set; }
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
        public string ItemName { get; set; }

        [JsonPropertyName("profile_id")]
        public string ProfileId { get; set; }

        [JsonPropertyName("item_lore")]
        public string ItemLore { get; set; }

        [JsonPropertyName("extra")]
        public string Extra { get; set; }

        [JsonPropertyName("category")]
        public string Category { get; set; }

        [JsonPropertyName("claimed")]
        public bool Claimed { get; set; }

        [JsonPropertyName("claimed_bidders")]
        public List<string> ClaimedBidders { get; set; }
        [JsonPropertyName("coop")]
        public List<string> Coop { get; set; }

        [JsonPropertyName("tier")]
        public string Tier { get; set; }

        [JsonPropertyName("starting_bid")]
        public long StartingBid { get; set; }

        [JsonPropertyName("item_bytes")]
        public string ItemBytes { get; set; }

        [JsonPropertyName("highest_bid_amount")]
        public long HighestBidAmount { get; set; }
        [JsonPropertyName("bids")]
        public List<Bids> Bids { get; set; }

        [JsonPropertyName("bin")]
        public bool BuyItNow { get; set; }
    }
}