using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Coflnet.Sky.Core;
using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using NUnit.Framework;

namespace Coflnet.Sky.Updater.Tests
{
    public class UpdaterTests
    {
        [Test]
        public void TimeFormat()
        {
            Assert.AreEqual("Thu, 20 Jan 2022 20:00:00 GMT", Updater.FormatTime(new System.DateTime(2022, 1, 20, 20, 0, 0, System.DateTimeKind.Utc)));
        }


        //[Test]
        public async Task Parse()
        {
            var update = new TestUpdater(null);

            var host = Program.CreateHostBuilder(new string[]{"--urls=http://localhost:6001/"}).Build();
            var cancleToken = new CancellationTokenSource(8000).Token;
            _ = host.RunAsync(cancleToken);
            await Task.Delay(200);
            // warmup
            await update.DoAnUpdate();

            var sw = Stopwatch.StartNew();
            await update.DoAnUpdate();

            var elapsed = sw.ElapsedMilliseconds;
            Assert.LessOrEqual(elapsed, 50);
            Assert.NotNull(update.producer.lastAuction);
            try 
            {
            await host.StopAsync();
            } catch(Exception e)
            {
                Console.WriteLine("failed to stop test host" + e);
            }
        }

        public class TestUpdater : NewUpdater
        {
            protected override string ApiBaseUrl => "http://localhost:6001";
            public FakeProducer producer = new FakeProducer();

            public TestUpdater(ActivitySource activitySource) : base(activitySource)
            {
            }

            public async Task DoAnUpdate()
            {

                await DoOneUpdate(DateTime.Now - TimeSpan.FromMinutes(1), producer, 0, null);
            }
        }

        public class FakeProducer : Confluent.Kafka.IProducer<string, Core.SaveAuction>
        {
            public SaveAuction lastAuction;
            public Handle Handle => throw new NotImplementedException();

            public string Name => throw new NotImplementedException();

            public void AbortTransaction(TimeSpan timeout)
            {
                throw new NotImplementedException();
            }

            public void AbortTransaction()
            {
                throw new NotImplementedException();
            }

            public int AddBrokers(string brokers)
            {
                throw new NotImplementedException();
            }

            public void BeginTransaction()
            {
                throw new NotImplementedException();
            }

            public void CommitTransaction(TimeSpan timeout)
            {
                throw new NotImplementedException();
            }

            public void CommitTransaction()
            {
                throw new NotImplementedException();
            }

            public void Dispose()
            {
                throw new NotImplementedException();
            }

            public int Flush(TimeSpan timeout)
            {
                return 1;
            }

            public void Flush(CancellationToken cancellationToken = default)
            {
                throw new NotImplementedException();
            }

            public void InitTransactions(TimeSpan timeout)
            {
                throw new NotImplementedException();
            }

            public int Poll(TimeSpan timeout)
            {
                throw new NotImplementedException();
            }

            public void Produce(string topic, Message<string, SaveAuction> message, Action<DeliveryReport<string, SaveAuction>> deliveryHandler = null)
            {
                lastAuction = message.Value;
                Console.WriteLine("Produced: " + message.Value.Uuid);
            }

            public void Produce(TopicPartition topicPartition, Message<string, SaveAuction> message, Action<DeliveryReport<string, SaveAuction>> deliveryHandler = null)
            {
                
            }

            public Task<DeliveryResult<string, SaveAuction>> ProduceAsync(string topic, Message<string, SaveAuction> message, CancellationToken cancellationToken = default)
            {
                throw new NotImplementedException();
            }

            public Task<DeliveryResult<string, SaveAuction>> ProduceAsync(TopicPartition topicPartition, Message<string, SaveAuction> message, CancellationToken cancellationToken = default)
            {
                throw new NotImplementedException();
            }

            public void SendOffsetsToTransaction(IEnumerable<TopicPartitionOffset> offsets, IConsumerGroupMetadata groupMetadata, TimeSpan timeout)
            {
                throw new NotImplementedException();
            }
        }
    }
}