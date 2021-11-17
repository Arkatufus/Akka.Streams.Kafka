using System;
using System.Collections.Immutable;
using Akka.Annotations;
using Akka.Streams.Stage;
using Confluent.Kafka;

namespace Akka.Streams.Kafka.Helpers
{
    /// <summary>
    /// The API is new and may change in further releases.
    ///
    /// Allows to execute user code when Kafka rebalances partitions between consumers, or an Alpakka Kafka consumer is stopped.
    /// Use with care: These callbacks are called synchronously on the same thread Kafka's `poll()` is called.
    /// A warning will be logged if a callback takes longer than the configured `partition-handler-warning`.
    ///
    /// There is no point in calling `CommittableOffset`'s commit methods as their committing won't be executed as long as any of
    /// the callbacks in this class are called.
    /// </summary>
    [ApiMayChange]
    public class PartitionAssignmentHandler
    {
        public static readonly PartitionAssignmentHandler Empty =
            new PartitionAssignmentHandler((p, c) => { }, (p, c) => { }, (p, c) => { }, (p, c) => { });

        public static PartitionAssignmentHandler Chain(PartitionAssignmentHandler handler1, PartitionAssignmentHandler handler2)
            => new PartitionAssignmentHandler(
                onRevoke: (p, c) =>
                {
                    handler1?.OnRevoke(p, c);
                    handler2?.OnRevoke(p, c);
                }, onAssign: (p, c) =>
                {
                    handler1?.OnAssign(p, c);
                    handler2?.OnAssign(p, c);
                }, onLost: (p, c) =>
                {
                    handler1?.OnLost(p, c);
                    handler2?.OnLost(p, c);
                }, onStop: (p, c) =>
                {
                    handler1?.OnStop(p, c);
                    handler2?.OnStop(p, c);
                });
        
        public PartitionAssignmentHandler(
            Action<IImmutableSet<TopicPartitionOffset>, IRestrictedConsumer> onRevoke,
            Action<IImmutableSet<TopicPartition>, IRestrictedConsumer> onAssign,
            Action<IImmutableSet<TopicPartition>, IRestrictedConsumer> onLost, 
            Action<IImmutableSet<TopicPartition>, IRestrictedConsumer> onStop)
        {
            OnRevoke = onRevoke;
            OnAssign = onAssign;
            OnLost = onLost;
            OnStop = onStop;
        }

        /// <summary>
        /// Called when partitions are revoked
        /// </summary>
        public Action<IImmutableSet<TopicPartitionOffset>, IRestrictedConsumer> OnRevoke { get; } 

        /// <summary>
        /// Called when partitions are assigned
        /// </summary>
        public Action<IImmutableSet<TopicPartition>, IRestrictedConsumer> OnAssign { get; }


        public Action<IImmutableSet<TopicPartition>, IRestrictedConsumer> OnLost { get; }
        
        /// <summary>
        /// Called when consuming is stopped
        /// </summary>
        public Action<IImmutableSet<TopicPartition>, IRestrictedConsumer> OnStop { get; }
    }
}