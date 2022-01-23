using NUnit.Framework;

namespace Coflnet.Sky.Updater.Tests
{
    public class UpdaterTests
    {
        [Test]
        public void TimeFormat()
        {
            Assert.AreEqual("Thu, 20 Jan 2022 20:00:00 GMT", Updater.FormatTime(new System.DateTime(2022,1,20,20,0,0, System.DateTimeKind.Utc)));
        }
    }
}