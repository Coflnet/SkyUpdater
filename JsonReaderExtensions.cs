using System.Collections.Generic;
using System.Text.RegularExpressions;
using Newtonsoft.Json;

namespace Coflnet.Sky.Updater
{
    public static class JsonReaderExtensions
    {
        public static async IAsyncEnumerable<T> SelectTokensWithRegex<T>(
            this JsonReader jsonReader, Regex regex)
        {
            JsonSerializer serializer = new JsonSerializer();
            while (await jsonReader.ReadAsync())
            {
                if (regex.IsMatch(jsonReader.Path)
                    && jsonReader.TokenType != JsonToken.PropertyName)
                {
                    yield return serializer.Deserialize<T>(jsonReader);
                }
            }
        }
    }
}
