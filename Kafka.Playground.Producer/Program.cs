using System;
using System.Threading.Tasks;

namespace Kafka.Playground.Producer
{
    class Program
    {
        static async Task Main(string[] args)
        {
            Console.WriteLine("[Producer]");
            string message;
            while ((message = Console.ReadLine()) != "q")
            {
                var producer = new Producer();
                await producer.Produce(message);
            }
        }
    }
}
