using System.Threading.Tasks;

namespace Kafka.Playground.Producer
{
    public interface IProducer
    {
        Task Produce(string message);
    }
}