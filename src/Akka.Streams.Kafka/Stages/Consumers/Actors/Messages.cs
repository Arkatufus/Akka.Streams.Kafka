using System.Collections.Immutable;
using System.Threading;
using Akka.Actor;
using Akka.Event;
using Confluent.Kafka;

namespace Akka.Streams.Kafka.Stages.Consumers.Actors
{
    public static class KafkaConsumerActor
    {
        /// <summary>
        /// Marker interface for subscription requests
        /// </summary>
        public interface ISubscriptionRequest
        {
        }

        #region requests
        public sealed class Assign: ISubscriptionRequest
        {
            /// <summary>
            /// Assign
            /// </summary>
            /// <param name="topicPartitions">Topic partitions</param>
            public Assign(IImmutableSet<TopicPartition> topicPartitions)
            {
                TopicPartitions = topicPartitions;
            }

            /// <summary>
            /// Topic partitions
            /// </summary>
            public IImmutableSet<TopicPartition> TopicPartitions { get; }
        }

        public sealed class AssignWithOffset: ISubscriptionRequest
        {
            /// <summary>
            /// AssignWithOffset
            /// </summary>
            /// <param name="topicPartitionOffsets">Topic partitions with offsets</param>
            public AssignWithOffset(IImmutableSet<TopicPartitionOffset> topicPartitionOffsets)
            {
                TopicPartitionOffsets = topicPartitionOffsets;
            }

            /// <summary>
            /// Topic partitions
            /// </summary>
            public IImmutableSet<TopicPartitionOffset> TopicPartitionOffsets { get; }
        }

        public sealed class AssignOffsetsForTimes: ISubscriptionRequest
        {
            public AssignOffsetsForTimes(ImmutableDictionary<TopicPartition, long> topicPartitions)
            {
                TopicPartitions = topicPartitions;
            }

            public ImmutableDictionary<TopicPartition, long> TopicPartitions { get; }
        }
        
        public sealed class Subscribe : ISubscriptionRequest
        {
            /// <summary>
            /// Subscribe
            /// </summary>
            public Subscribe(IImmutableSet<string> topics)
            {
                Topics = topics;
            }

            /// <summary>
            /// List of topics to subscribe
            /// </summary>
            public IImmutableSet<string> Topics { get; }
        }

        // Not implemented
        /*
        public sealed class RequestMetrics : INoSerializationVerificationNeeded
        {
            public static readonly RequestMetrics Instance = new RequestMetrics();
            private RequestMetrics(){}
        }
        */

        public sealed class SubscribePattern : ISubscriptionRequest
        {
            /// <summary>
            /// SubscribePattern
            /// </summary>
            /// <param name="topicPattern">Topic pattern (regular expression to be matched)</param>
            public SubscribePattern(string topicPattern)
            {
                TopicPattern = topicPattern;
            }

            /// <summary>
            /// Topic pattern (regular expression to be matched)
            /// </summary>
            public string TopicPattern { get; }
        }

        public sealed class RegisterSubStage: INoSerializationVerificationNeeded
        {
            public RegisterSubStage(ImmutableHashSet<TopicPartition> topicPartitions)
            {
                TopicPartitions = topicPartitions;
            }

            public ImmutableHashSet<TopicPartition> TopicPartitions { get; }
        }
        
        public sealed class Seek: INoSerializationVerificationNeeded
        {
            /// <summary>
            /// Seek
            /// </summary>
            /// <param name="offsets">Offsets to seek</param>
            public Seek(IImmutableSet<TopicPartitionOffset> offsets)
            {
                Offsets = offsets;
            }

            /// <summary>
            /// Offsets to seek
            /// </summary>
            public IImmutableSet<TopicPartitionOffset> Offsets { get; }
        }

        /// <summary>
        /// Used to request for kafka messages
        /// </summary>
        public class RequestMessages: INoSerializationVerificationNeeded
        {
            /// <summary>
            /// RequestMessages
            /// </summary>
            /// <param name="requestId">Request Id</param>
            /// <param name="topics">List of topics to consume</param>
            public RequestMessages(int requestId, ImmutableHashSet<TopicPartition> topics)
            {
                RequestId = requestId;
                Topics = topics;
            }

            /// <summary>
            /// Request Id
            /// </summary>
            public int RequestId { get; }

            /// <summary>
            /// List of topics to consume
            /// </summary>
            public ImmutableHashSet<TopicPartition> Topics { get; }
        }

        public interface IStopLike
        { }

        /// <summary>
        /// Stops consuming actor
        /// </summary>
        public class Stop : IStopLike, INoSerializationVerificationNeeded
        {
            public static readonly Stop Instance = new Stop();
            private Stop() { }
        }

        public class StopFromStage : IStopLike, INoSerializationVerificationNeeded
        {
            public StopFromStage(string stageId)
            {
                StageId = stageId;
            }

            public string StageId { get; }
        }
        
        public class Commit : INoSerializationVerificationNeeded
        {
            /// <summary>
            /// Commit
            /// </summary>
            /// <param name="offsets">List of offsets to commit</param>
            public Commit(IImmutableSet<TopicPartitionOffset> offsets)
            {
                Offsets = offsets;
            }

            /// <summary>
            /// List of offsets to commit
            /// </summary>
            public IImmutableSet<TopicPartitionOffset> Offsets { get; }
        }

        public class CommitWithoutReply : INoSerializationVerificationNeeded
        {
            public CommitWithoutReply(TopicPartitionOffset offset, bool emergency)
            {
                Offset = offset;
                Emergency = emergency;
            }
            
            public TopicPartitionOffset Offset { get; }
            public bool Emergency { get; }
        }

        // Special case commit for non-batched committing.
        public class CommitSingle : INoSerializationVerificationNeeded
        {
            public CommitSingle(TopicPartitionOffset offset)
            {
                Offset = offset;
            }

            public TopicPartitionOffset Offset { get; }
        }
        #endregion

        #region responses

        public sealed class Assigned : INoSerializationVerificationNeeded
        {
            public Assigned(IImmutableSet<TopicPartition> partitions)
            {
                Partitions = partitions;
            }

            public IImmutableSet<TopicPartition> Partitions { get; }
        }
        
        /// <summary>
        /// Revoked
        /// </summary>
        public class Revoked: INoSerializationVerificationNeeded
        {
            /// <summary>
            /// Revoked
            /// </summary>
            /// <param name="partitions">List of revoked partitions</param>
            public Revoked(IImmutableSet<TopicPartition> partitions)
            {
                Partitions = partitions;
            }

            /// <summary>
            /// List of revoked partitions
            /// </summary>
            public IImmutableSet<TopicPartition> Partitions { get; }
        }

        public class Messages<K, V>
        {
            /// <summary>
            /// Messages
            /// </summary>
            /// <param name="requestId">Request Id</param>
            /// <param name="messagesList">List of consumed messages</param>
            public Messages(int requestId, ImmutableList<ConsumeResult<K, V>> messagesList)
            {
                RequestId = requestId;
                MessagesList = messagesList;
            }

            /// <summary>
            /// Request Id
            /// </summary>
            public int RequestId { get; }

            /// <summary>
            /// List of consumed messages
            /// </summary>
            public ImmutableList<ConsumeResult<K, V>> MessagesList { get; }
        }

        // Not implemented
        /*
        public sealed class ConsumerMetrics
        {
            
        }
        */

        #endregion

        
        /// <summary>
        /// Used to send commit requests to <see cref="KafkaConsumerActor{K,V}"/>
        /// </summary>

        /// <summary>
        /// Committed
        /// </summary>
        public class Committed
        {
            /// <summary>
            /// Commited message
            /// </summary>
            /// <param name="offsets">Collection of committed offsets</param>
            public Committed(IImmutableSet<TopicPartitionOffset> offsets)
            {
                Offsets = offsets;
            }

            /// <summary>
            /// Committed offsets
            /// </summary>
            public IImmutableSet<TopicPartitionOffset> Offsets { get; }
        }

        #region internal

        internal sealed class Poll<TKey, TValue> : IDeadLetterSuppression, INoSerializationVerificationNeeded 
        {
            public Poll(KafkaConsumerActor<TKey, TValue> target, bool periodic)
            {
                Target = target;
                Periodic = periodic;
            }

            public KafkaConsumerActor<TKey, TValue> Target { get; }
            public bool Periodic { get; }
        }
        
        internal sealed class PollTask
        {
            public static readonly PollTask Instance = new PollTask();
            private PollTask(){}
        }

        private static volatile int _number = 1;
        /// <summary>
        /// Gets next actor number in thread-safe way
        /// </summary>
        /// <returns></returns>
        public static int NextNumber() => Interlocked.Increment(ref _number);

        #endregion
    }
}