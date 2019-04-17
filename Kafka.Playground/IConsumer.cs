using System;

namespace Kafka.Playground.Consumer
{
    public interface IConsumer
    {
        void Subscribe(string topic, Action<string> handler);

        void Unsubscribe(string topic);
    }
}