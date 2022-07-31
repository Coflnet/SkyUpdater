using System.Collections.Generic;
using System.Text.RegularExpressions;
using Coflnet.Sky.Updater.Models;
using Newtonsoft.Json;

namespace Coflnet.Sky.Updater
{
    public static class JsonReaderExtensions
    {
        public static async IAsyncEnumerable<T> SelectTokensWithRegex<T>(
            this JsonReader jsonReader, Regex regex)
        {
            JsonSerializer serializer = new JsonSerializer();
            while (await jsonReader.ReadAsync().ConfigureAwait(false))
            {
                if (jsonReader.TokenType != JsonToken.PropertyName && regex.IsMatch(jsonReader.Path)
                    )
                {
                    yield return serializer.Deserialize<T>(jsonReader);
                }
            }
        }
    }
}
