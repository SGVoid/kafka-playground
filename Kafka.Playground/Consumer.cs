using Confluent.Kafka;
using Confluent.Kafka.Serialization;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Kafka.Playground.Consumer
{
    public class Consumer : IConsumer
    {
        public Action<string> Handler { get; private set; }

        private readonly IReadOnlyDictionary<string, object> _config = new Dictionary<string, object>
        {
            {"group.id", $"consumer_{Process.GetCurrentProcess().Id}"},
            {"bootstrap.servers", "localhost:9092"},
            {"enable.auto.commit","true" }
        };

        private CancellationTokenSource _cancellationTokenSource;
        private bool _subscribed = false;

        public void Subscribe(string topic, Action<string> handler)
        {
            if (_subscribed)
            {
                Console.WriteLine("Already subscribed, returning ...");
                return;
            }

            _subscribed = true;
            _cancellationTokenSource = new CancellationTokenSource();

            Task.Factory.StartNew(() => RunKafkaPolling(topic, handler, _cancellationTokenSource.Token), _cancellationTokenSource.Token);
        }

        private Task RunKafkaPolling(string topic, Action<string> handler, CancellationToken token)
        {
            Handler = handler;

            using (var consumer = new Consumer<Null, string>(_config, null, new StringDeserializer(Encoding.UTF8)))
            {
                consumer.Subscribe(topic);

                consumer.OnMessage += Consumer_OnMessage;
                consumer.OnError += Consumer_OnError;

                while (!token.IsCancellationRequested)
                {
                    consumer.Poll(TimeSpan.FromMilliseconds(1000));
                }

                consumer.OnMessage -= Consumer_OnMessage;
                consumer.OnError -= Consumer_OnError;

                consumer.Unsubscribe();
            }

            return Task.CompletedTask;
        }

        private void Consumer_OnMessage(object sender, Message<Null, string> e)
        {
            Handler?.Invoke(e.Value);
        }

        private void Consumer_OnError(object sender, Error e)
        {
            Console.ForegroundColor = ConsoleColor.DarkMagenta;
            Console.WriteLine(e.Reason);
            Console.ForegroundColor = ConsoleColor.White;
        }

        public void Unsubscribe(string topic)
        {
            if (!_subscribed)
            {
                Console.WriteLine("Already Unsubscribed, returning ...");
                return;
            }

            _subscribed = false;
            _cancellationTokenSource?.Cancel();
        }
    }
}