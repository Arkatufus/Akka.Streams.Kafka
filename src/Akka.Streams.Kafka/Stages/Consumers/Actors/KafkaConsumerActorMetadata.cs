using System.Collections.Immutable;
using System.Threading;
using Akka.Actor;
using Akka.Annotations;
using Akka.Streams.Kafka.Helpers;
using Akka.Streams.Kafka.Settings;
using Confluent.Kafka;

namespace Akka.Streams.Kafka.Stages.Consumers.Actors
{
    /// <summary>
    /// Containes metadata for <see cref="KafkaConsumerActor{K,V}"/>.
    /// Generally this should not be used from outside of the library.
    /// </summary>
    [InternalApi]
    public static class KafkaConsumerActorMetadata
    {
        /// <summary>
        /// Gets actor props
        /// </summary>
        public static Props GetProps<K, V>(ConsumerSettings<K, V> settings) =>
            Props.Create(() => new KafkaConsumerActor<K, V>(ActorRefs.Nobody, settings, PartitionAssignmentHandler.Empty, new StatisticsHandlers.Empty())).WithDispatcher(settings.DispatcherId);
        
        /// <summary>
        /// Gets actor props
        /// </summary>
        internal static Props GetProps<K, V>(ConsumerSettings<K, V> settings, PartitionAssignmentHandler handler, IStatisticsHandler statisticsHandler) =>
            Props.Create(() => new KafkaConsumerActor<K, V>(ActorRefs.Nobody, settings, handler, statisticsHandler)).WithDispatcher(settings.DispatcherId);
        
        /// <summary>
        /// Gets actor props
        /// </summary>
        internal static Props GetProps<K, V>(IActorRef owner, ConsumerSettings<K, V> settings, PartitionAssignmentHandler handler, IStatisticsHandler statisticsHandler) =>
            Props.Create(() => new KafkaConsumerActor<K, V>(owner, settings, handler, statisticsHandler)).WithDispatcher(settings.DispatcherId);
    }
}