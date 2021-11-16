using System;
using System.Collections.Generic;
using System.Threading;
using Akka.Actor;
using Akka.Streams.Kafka.Settings;
using Confluent.Kafka;

namespace Akka.Streams.Kafka.Internal
{
    public static class ConsumerFacade
    {
        public static readonly Error NotInitializedError =
            new Error(ErrorCode.Unknown, "Consumer has not been subscribed to", true);
        
        public class NotInitialized : KafkaException
        {
            public NotInitialized() : base(NotInitializedError)
            {
            }

            public NotInitialized(Exception innerException) : base(NotInitializedError, innerException)
            {
            }
        }
    }

    public interface IPartitionAssignmentHandler<K, V>
    {
        public void OnRevoke(IConsumer<K, V> consumer, List<TopicPartitionOffset> revokedTps);
        public void OnAssign(IConsumer<K, V> consumer, List<TopicPartition> assignedTps);
        public void OnLost(IConsumer<K, V> consumer, List<TopicPartitionOffset> lostTps);
        public void OnStop(IConsumer<K, V> consumer, List<TopicPartition> currentTps);
    }

    public class ConsumerFacade<K, V> : IConsumer<K, V>
    {
        private readonly ConsumerSettings<K, V> _settings;
        private IPartitionAssignmentHandler<K, V> _callbacks;
        private IConsumer<K, V> _internalConsumer = null;

        public ConsumerFacade(ConsumerSettings<K, V> settings)
        {
            _settings = settings;
        }

        public void Dispose() => _internalConsumer?.Dispose();

        public int AddBrokers(string brokers)
        {
            return _internalConsumer?.AddBrokers(brokers) ?? throw new ConsumerFacade.NotInitialized();
        }

        public Handle Handle => _internalConsumer.Handle ?? throw new ConsumerFacade.NotInitialized();
        public string Name => _internalConsumer.Name ?? throw new ConsumerFacade.NotInitialized();
        
        public ConsumeResult<K, V> Consume(int millisecondsTimeout)
            => _internalConsumer?.Consume(millisecondsTimeout)?? throw new ConsumerFacade.NotInitialized();

        public ConsumeResult<K, V> Consume(CancellationToken cancellationToken) 
            => _internalConsumer?.Consume(cancellationToken) ?? throw new ConsumerFacade.NotInitialized();

        public ConsumeResult<K, V> Consume(TimeSpan timeout) 
            => _internalConsumer?.Consume(timeout) ?? throw new ConsumerFacade.NotInitialized();

        public List<TopicPartitionOffset> Committed(TimeSpan timeout) 
            => _internalConsumer?.Committed(timeout) ?? throw new ConsumerFacade.NotInitialized();

        public List<TopicPartitionOffset> Committed(IEnumerable<TopicPartition> partitions, TimeSpan timeout) 
            => _internalConsumer?.Committed(partitions, timeout) ?? throw new ConsumerFacade.NotInitialized();

        public Offset Position(TopicPartition partition) 
            => _internalConsumer?.Position(partition) ?? throw new ConsumerFacade.NotInitialized();

        public List<TopicPartitionOffset> OffsetsForTimes(IEnumerable<TopicPartitionTimestamp> timestampsToSearch, TimeSpan timeout) 
            => _internalConsumer.OffsetsForTimes(timestampsToSearch, timeout) ?? throw new ConsumerFacade.NotInitialized();

        public WatermarkOffsets GetWatermarkOffsets(TopicPartition topicPartition) 
            => _internalConsumer?.GetWatermarkOffsets(topicPartition) ?? throw new ConsumerFacade.NotInitialized();

        public WatermarkOffsets QueryWatermarkOffsets(TopicPartition topicPartition, TimeSpan timeout) 
            => _internalConsumer?.QueryWatermarkOffsets(topicPartition, timeout) ?? throw new ConsumerFacade.NotInitialized();

        public string MemberId => _internalConsumer?.MemberId ?? throw new ConsumerFacade.NotInitialized();
        public List<TopicPartition> Assignment => _internalConsumer?.Assignment ?? throw new ConsumerFacade.NotInitialized();
        public List<string> Subscription => _internalConsumer?.Subscription ?? throw new ConsumerFacade.NotInitialized();
        public IConsumerGroupMetadata ConsumerGroupMetadata => _internalConsumer?.ConsumerGroupMetadata ?? throw new ConsumerFacade.NotInitialized();
        
        public List<TopicPartitionOffset> Commit() 
            => _internalConsumer?.Commit() ?? throw new ConsumerFacade.NotInitialized();

        public void Subscribe(IEnumerable<string> topics, IPartitionAssignmentHandler<K, V> callbacks)
        {
            _internalConsumer?.Dispose();
            _callbacks = callbacks;
            var builder = ConsumerSettings<K, V>.CreateKafkaConsumerBuilder(_settings);
            
        }
        
        void IConsumer<K, V>.Subscribe(IEnumerable<string> topics) => throw new NotImplementedException();

        void IConsumer<K, V>.Subscribe(string topic) => throw new NotImplementedException();

        void IConsumer<K, V>.Unsubscribe() => throw new NotImplementedException();

        public void Assign(TopicPartition partition)
        {
            if(_internalConsumer == null)
                throw new ConsumerFacade.NotInitialized();
            _internalConsumer.Assign(partition);
        }

        public void Assign(TopicPartitionOffset partition)
        {
            if(_internalConsumer == null)
                throw new ConsumerFacade.NotInitialized();
            _internalConsumer.Assign(partition);
        }

        public void Assign(IEnumerable<TopicPartitionOffset> partitions)
        {
            if(_internalConsumer == null)
                throw new ConsumerFacade.NotInitialized();
            _internalConsumer.Assign(partitions);
        }

        public void Assign(IEnumerable<TopicPartition> partitions)
        {
            if(_internalConsumer == null)
                throw new ConsumerFacade.NotInitialized();
            _internalConsumer.Assign(partitions);
        }

        public void IncrementalAssign(IEnumerable<TopicPartitionOffset> partitions)
        {
            if(_internalConsumer == null)
                throw new ConsumerFacade.NotInitialized();
            _internalConsumer.IncrementalAssign(partitions);
        }

        public void IncrementalAssign(IEnumerable<TopicPartition> partitions)        
        {
            if(_internalConsumer == null)
                throw new ConsumerFacade.NotInitialized();
            _internalConsumer.IncrementalAssign(partitions);
        }

        public void IncrementalUnassign(IEnumerable<TopicPartition> partitions)        
        {
            if(_internalConsumer == null)
                throw new ConsumerFacade.NotInitialized();
            _internalConsumer.IncrementalUnassign(partitions);
        }

        public void Unassign()        
        {
            if(_internalConsumer == null)
                throw new ConsumerFacade.NotInitialized();
            _internalConsumer.Unassign();
        }

        public void StoreOffset(ConsumeResult<K, V> result)        
        {
            if(_internalConsumer == null)
                throw new ConsumerFacade.NotInitialized();
            _internalConsumer.StoreOffset(result);
        }

        public void StoreOffset(TopicPartitionOffset offset)        
        {
            if(_internalConsumer == null)
                throw new ConsumerFacade.NotInitialized();
            _internalConsumer.StoreOffset(offset);
        }

        public void Commit(IEnumerable<TopicPartitionOffset> offsets)        
        {
            if(_internalConsumer == null)
                throw new ConsumerFacade.NotInitialized();
            _internalConsumer.Commit(offsets);
        }

        public void Commit(ConsumeResult<K, V> result)        
        {
            if(_internalConsumer == null)
                throw new ConsumerFacade.NotInitialized();
            _internalConsumer.Commit(result);
        }

        public void Seek(TopicPartitionOffset tpo)        
        {
            if(_internalConsumer == null)
                throw new ConsumerFacade.NotInitialized();
            _internalConsumer.Seek(tpo);
        }

        public void Pause(IEnumerable<TopicPartition> partitions)        
        {
            if(_internalConsumer == null)
                throw new ConsumerFacade.NotInitialized();
            _internalConsumer.Pause(partitions);
        }

        public void Resume(IEnumerable<TopicPartition> partitions)        
        {
            if(_internalConsumer == null)
                throw new ConsumerFacade.NotInitialized();
            _internalConsumer.Resume(partitions);
        }

        public void Close()        
        {
            if(_internalConsumer == null)
                throw new ConsumerFacade.NotInitialized();
            _internalConsumer.Close();
        }

    }
}