using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Confluent.Kafka;
using Kafka.Playground.Common;

namespace Kafka.Playground.Producer
{
    public class Producer : IProducer
    {
        public async Task Produce(string message)
        {
            var config = new Dictionary<string, string>()
            { 
                {"bootstrap.servers", "localhost:9092"}
            };

            using (var producer = new ProducerBuilder<string, string>(config).SetErrorHandler(ErrorHandler).Build())
            {
                Console.WriteLine($"{DateTime.UtcNow} | Publishing ...");

                var deliveryReport = await producer.ProduceAsync(Contract.TopicName, new Message<string, string> { Key = null, Value = message });

                Console.WriteLine($"| {DateTime.UtcNow} " +
                                  $"| published | {message} " +
                                  $"| {deliveryReport.TopicPartitionOffset} " +
                                  $"| {deliveryReport.Partition.Value}");
            }
        }

        private void ErrorHandler(Producer<string, string> producer, Error error)
        {
            Console.WriteLine(error.ToString());
        }
    }
}