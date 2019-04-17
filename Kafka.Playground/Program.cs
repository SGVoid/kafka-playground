using System;
using Kafka.Playground.Common;

namespace Kafka.Playground.Consumer
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("[Consumer]");

            var consumer = new Consumer();

            while (true)
            {
                var value = Console.ReadLine();

                switch (value)
                {
                    case "u":
                        consumer.Unsubscribe(Contract.TopicName);
                        Console.WriteLine($"\t[Unsubscribed from {Contract.TopicName}]");
                        continue;

                    case "s":
                        consumer.Subscribe(Contract.TopicName, Console.WriteLine);
                        Console.WriteLine($"\t[Subscribed to {Contract.TopicName}]");
                        continue;

                    case "q":
                        break;
                }
            }
        }
    }
}
