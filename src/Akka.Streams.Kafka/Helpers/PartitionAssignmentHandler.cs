using System;
using System.Collections.Immutable;
using Akka.Actor;
using Akka.Annotations;
using Akka.Streams.Kafka.Settings;
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

        protected PartitionAssignmentHandler() { }
        
        public PartitionAssignmentHandler(
            Action<IImmutableSet<TopicPartitionOffset>, IRestrictedConsumer> onRevoke,
            Action<IImmutableSet<TopicPartition>, IRestrictedConsumer> onAssign,
            Action<IImmutableSet<TopicPartitionOffset>, IRestrictedConsumer> onLost, 
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
        public Action<IImmutableSet<TopicPartitionOffset>, IRestrictedConsumer> OnRevoke { get; protected set; } 

        /// <summary>
        /// Called when partitions are assigned
        /// </summary>
        public Action<IImmutableSet<TopicPartition>, IRestrictedConsumer> OnAssign { get; protected set;}


        public Action<IImmutableSet<TopicPartitionOffset>, IRestrictedConsumer> OnLost { get; protected set;}
        
        /// <summary>
        /// Called when consuming is stopped
        /// </summary>
        public Action<IImmutableSet<TopicPartition>, IRestrictedConsumer> OnStop { get; protected set;}
    }

    public class AsyncCallbacks : PartitionAssignmentHandler
    {
        private readonly IAutoSubscription _subscription;
        private readonly IActorRef _sourceActor;
        private AsyncCallback<IImmutableSet<TopicPartition>> _partitionAssignedCb;
        private AsyncCallback<IImmutableSet<TopicPartitionOffset>> _partitionRevokeddCb;

        public AsyncCallbacks(
            IAutoSubscription subscription,
            IActorRef sourceActor,
            AsyncCallback<IImmutableSet<TopicPartition>> partitionAssignedCb,
            AsyncCallback<IImmutableSet<TopicPartitionOffset>> partitionRevokedCb)
        {
            _subscription = subscription;
            _sourceActor = sourceActor;
            _partitionAssignedCb = partitionAssignedCb;
            _partitionRevokeddCb = partitionRevokedCb;

            OnRevoke = (revokedTps, consumer) =>
            {
                subscription.RebalanceListener?.Tell(new TopicPartitionsRevoked(_subscription, revokedTps), _sourceActor);
                if (revokedTps.Count > 0)
                {
                    _partitionRevokeddCb.Invoke(revokedTps);
                }
            };

            OnAssign = (assignedTps, consumer) =>
            {
                subscription.RebalanceListener?.Tell(new TopicPatririonsAssigned(_subscription, assignedTps),
                    _sourceActor);
                if (assignedTps.Count > 0)
                {
                    _partitionAssignedCb.Invoke(assignedTps);
                }
            };

            OnLost = (lostTps, consumer) => OnRevoke(lostTps, consumer);

            OnStop = (_, _) => { };
        }

        public override string ToString() => $"AsyncCallbacks({_subscription}, {_sourceActor})";
    }

    public class Chain : PartitionAssignmentHandler
    {
        public Chain(PartitionAssignmentHandler handler1, PartitionAssignmentHandler handler2)
        {
            OnRevoke =  (p, c) =>
            {
                handler1?.OnRevoke(p, c);
                handler2?.OnRevoke(p, c);
            };

            OnAssign = (p, c) =>
            {
                handler1?.OnAssign(p, c);
                handler2?.OnAssign(p, c);
            };
            OnLost = (p, c) =>
            {
                handler1?.OnLost(p, c);
                handler2?.OnLost(p, c);
            }; 
            
            OnStop = (p, c) =>
            {
                handler1?.OnStop(p, c);
                handler2?.OnStop(p, c);
            };
        }
    }
}