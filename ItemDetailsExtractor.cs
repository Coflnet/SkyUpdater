using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Coflnet.Sky.Updater.Models;
using dev;
using Coflnet.Sky.Core;
using Newtonsoft.Json;
using static Coflnet.Sky.Core.ItemDetails;

namespace Coflnet.Sky.Updater
{
    public class ItemDetailsExtractor
    {
        public ConcurrentDictionary<string, ItemDetails.Item> Items => ItemDetails.Instance.Items;
        private static ConcurrentDictionary<string, DBItem> ToFillDetails = new ConcurrentDictionary<string, DBItem>();

        public void LoadToFill()
        {
            using (var context = new HypixelContext())
            {
                var items = context.Items.Where(item => item.Description == null);
                foreach (var item in items)
                {
                    ToFillDetails.TryAdd(item.Tag, item);
                }
            }
        }
        
        public void AddOrIgnoreDetails(Auction a)
        {
            var id = NBT.ItemID(a.ItemBytes);
            if (id == null)
            {
                if (a.ItemName == "Revive Stone")
                {
                    // known item, has no tag, nothing to do
                    return;
                }
                Logger.Instance.Error($"item has no tag {JsonConvert.SerializeObject(a)}");
                return;
            }

            var name = ItemReferences.RemoveReforgesAndLevel(a.ItemName);

            if (ToFillDetails.TryRemove(id, out DBItem i))
            {
                Console.WriteLine("Filling details for " + i.Tag + i.Id);
                AddNewItem(a, name, id, i);
                return;
            }
            if (Items.ContainsKey(id))
            {
                var tragetItem = Items[id];
                if (tragetItem.AltNames == null)
                    tragetItem.AltNames = new HashSet<string>();

                // try to get shorter lore
                if (Items[id]?.Description?.Length > a?.ItemLore?.Length && a.ItemLore.Length > 10)
                {
                    Items[id].Description = a.ItemLore;
                }
                tragetItem.AltNames.Add(name);
                return;
            }
            // legacy item names
            if (Items.ContainsKey(name))
            {
                var item = Items[name];
                item.Id = id;
                if (item.AltNames == null)
                {
                    item.AltNames = new HashSet<string>();
                }
                item.AltNames.Add(name);
                Items[id] = item;
                Items.Remove(name, out _);

                return;
            }

            // new item, add it
            AddNewItem(a, name, id, i);
        }

        private void AddNewItem(Auction a, string name, string tag, DBItem existingItem = null)
        {
            var i = new ItemDetails.Item();
            i.Id = tag;
            i.AltNames = new HashSet<string>() { name };
            i.Tier = a.Tier;
            i.Category = a.Category;
            i.Description = a.ItemLore;
            i.Extra = a.Extra;
            if (a?.Uuid[0] == 'a')
                Console.WriteLine("minecraft types are currently disabled");
            //i.MinecraftType = MinecraftTypeParser.Instance.Parse(a);
            i.MinecraftType = "";

            //SetIconUrl(a, i);

            ItemDetails.Instance.Items[name] = i;
            var newItem = new DBItem(i);

            System.Threading.Tasks.Task.Run(async () =>
            {
                if (existingItem == null)
                    ItemDetails.Instance.AddItemToDB(newItem);
                else
                    await UpdateItem(existingItem, newItem);
            }).ConfigureAwait(false); ;
        }

        private async Task UpdateItem(DBItem existingItem, DBItem newItem)
        {
            await Task.Delay(5000);
            Console.WriteLine("updating item");
            using (var context = new HypixelContext())
            {
                newItem.Id = existingItem.Id;
                context.Items.Update(newItem);
                await context.SaveChangesAsync();
            }
        }

        private static void SetIconUrl(Auction a, IItem i)
        {
            if (i.MinecraftType == "skull")
            {
                try
                {
                    i.IconUrl = $"https://mc-heads.net/head/{Path.GetFileName(NBT.SkullUrl(a.ItemBytes))}/50";
                }
                catch (Exception e)
                {
                    Console.WriteLine($"Error :O \n {e.Message} \n {e.StackTrace}");
                }
            }
            else
            {
                var t = MinecraftTypeParser.Instance.GetDetails(i.MinecraftType);
                i.IconUrl = $"https://sky.coflnet.com/static/{t?.type}-{t?.meta}.png";
            }
        }
    }
}
