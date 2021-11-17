using Akka.Streams.Kafka.Internal;
using Akka.Streams.Kafka.Settings;
using Confluent.Kafka;

namespace Akka.Streams.Kafka
{
    public sealed class ConsumerFactory<K, V>
    {
        public static ConsumerFactory<K, V> Empty = new ConsumerFactory<K, V>(EmptyConsumerRebalanceListener.Instance);
        
        public ConsumerFactory(IConsumerRebalanceListener consumerRebalanceListener)
        {
            _consumerRebalanceListener = consumerRebalanceListener;
        }

        private readonly IConsumerRebalanceListener _consumerRebalanceListener;  
        
        internal IConsumer<K, V> Create(ConsumerSettings<K, V> settings)
        {
            return new ConsumerFacade<K, V>(settings, _consumerRebalanceListener);
        }
    }
}