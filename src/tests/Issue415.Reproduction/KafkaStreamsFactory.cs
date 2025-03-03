using System.Collections.Immutable;
using Akka;
using Akka.Actor;
using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.Streams.Kafka.Dsl;
using Akka.Streams.Kafka.Helpers;
using Akka.Streams.Kafka.Messages;
using Akka.Streams.Kafka.Settings;
using Akka.Util;
using Confluent.Kafka;
using Directive = Akka.Streams.Supervision.Directive;

namespace Issue415.Reproduction;

public class KafkaStreamFactory<T>
{
    private readonly KafkaSourceDecider _sourceDecider;
    private readonly string _topic;
    private readonly ConsumerSettings<Ignore, byte[]> _consumerSettings;
    private readonly IPartitionEventHandler _partitionEventHandler;

    public KafkaStreamFactory(
        KafkaSourceDecider sourceDecider,
        string topic,
        ConsumerSettings<Ignore, byte[]> consumerSettings,
        IPartitionEventHandler partitionEventHandler)
    {
        _sourceDecider = sourceDecider;
        _topic = topic;
        _consumerSettings = consumerSettings;
        _partitionEventHandler = partitionEventHandler;
    }
    
    public IKillSwitch CreateKillableStream(IUntypedActorContext context,
        Func<CommittableMessage<Ignore, byte[]>, Task<ICommittable>> processingFunction)
    {
        var source = GetSource();
        var flow = GetFlow(processingFunction);
        var sink = GetSink(context.System);
        var restartSettings = GetRestartSettings();

        var restartSource = RestartSource.OnFailuresWithBackoff(() => source.Via(flow), restartSettings);
        var restartCommitSink = RestartSink.WithBackoff(() => sink, restartSettings);

        return restartSource
            .ViaMaterialized(KillSwitches.Single<ICommittable>(), Keep.Right)
            .ToMaterialized(restartCommitSink, Keep.Left)
            .Run(context.Materializer());
    }
    
    private Flow<CommittableMessage<Ignore, byte[]>, ICommittable, Task<ICommittableOffset>> GetFlow(
        Func<CommittableMessage<Ignore, byte[]>, Task<ICommittable>> processingFunction)
    {
        return Flow
            .Create<CommittableMessage<Ignore, byte[]>, Task<ICommittableOffset>>()
            .SelectAsync(20, processingFunction)
            .Named($"{typeof(T).Name}-flow");
    }
    
    private Sink<ICommittable, Task<Done>> GetSink(ActorSystem actorSystem)
    {
        return Committer
            .BatchFlow(CommitterSettings.Create(actorSystem))
            .SelectMany(batch => batch.Offsets)
            .Select(_ => Done.Instance)
            .ToMaterialized(Sink.Ignore<Done>(), Keep.Right);
    }
    
    private Source<CommittableMessage<Ignore, byte[]>, IControl> GetSource()
    {
        var topicSubscription = Subscriptions.Topics(_topic);

        topicSubscription.WithPartitionEventsHandler(_partitionEventHandler);

        return KafkaConsumer
            .CommittableSource(_consumerSettings, topicSubscription)
            .WithAttributes(ActorAttributes.CreateSupervisionStrategy(_sourceDecider.Decide));
    }
    
    private static RestartSettings GetRestartSettings()
    {
        return RestartSettings.Create(TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(10), 0.2);
    }
}

public class KafkaSourceDecider
{
    private const Directive Stop = Directive.Stop;
    private const Directive Resume = Directive.Resume;
    private readonly ILoggingAdapter _logger;
    private readonly bool _autoCreateTopics;
    private readonly AtomicReference<Exception> _trigger;

    public KafkaSourceDecider(ILoggingAdapter logger, AtomicReference<Exception> trigger, bool autoCreateTopics = false)
    {
        _logger = logger;
        _trigger = trigger;
        _autoCreateTopics = autoCreateTopics;
    }
    
    /// <summary>
    /// The delegate method that is passed to the ActorAttributes.CreateSupervisionStrategy() method 
    /// </summary>
    /// <param name="exception">The exception thrown</param>
    /// <returns><see cref="Akka.Streams.Supervision.Directive"/>></returns>
    public Directive Decide(Exception exception)
    {
        _logger.Warning(exception, "Exception in Kafka Source.");
        switch (exception)
        {
            case ConsumeException ce:
                if (ce.Error.IsFatal)
                {
                    _logger.Info(exception, "Returning decision: {0} from decider because error marked as Fatal.", Stop);
                    return Stop;
                }

                // Workaround for https://github.com/confluentinc/confluent-kafka-dotnet/issues/1366
                if (ce.Error.Code == ErrorCode.UnknownTopicOrPart && _autoCreateTopics)
                    return Resume;

                if (ce.Error.IsSerializationError())
                {
                    _logger.Info(exception, "Returning decision: {0} from decider.", Stop);
                    return Stop;
                }

                _logger.Info(exception, "Returning decision: {0} from decider.", Resume);
                return Resume;
            
            case KafkaRetriableException _:
                _logger.Info(exception, "Returning decision: {0} from decider because of retriable exception.", Resume);
                return Resume;
            
            case KafkaException ke:
                if (ke.Error.IsFatal)
                {
                    _logger.Info(exception, "Returning decision: {0} from decider because error marked as Fatal.", Stop);
                    return Stop;
                }

                _logger.Info(exception, "Returning decision: {0} from decider.", Resume);
                return Resume;
            
            default:
                _trigger.CompareAndSet(null!, exception);
                _logger.Error(exception, "Exception in Kafka Source. Returning decision: {0} from decider.", Stop);
                return Stop;
        }
    }
}

public class CustomPartitionEventHandler(ILoggingAdapter log) : IPartitionEventHandler
{
    public void OnRevoke(IImmutableSet<TopicPartitionOffset> revokedTopicPartitions, IRestrictedConsumer consumer)
    {
        if(revokedTopicPartitions.Count == 0) 
            return;
        log.Info("Partitions revoked: {0}", string.Join(", ", revokedTopicPartitions));
    }

    public void OnLost(IImmutableSet<TopicPartitionOffset> revokedTopicPartitions, IRestrictedConsumer consumer)
    {
        if(revokedTopicPartitions.Count == 0) 
            return;
        log.Info("Partitions lost: {0}", string.Join(", ", revokedTopicPartitions));
    }

    public void OnAssign(IImmutableSet<TopicPartition> assignedTopicPartitions, IRestrictedConsumer consumer)
    {
        if(assignedTopicPartitions.Count == 0) 
            return;
        log.Info("Partitions assigned: {0}", string.Join(", ", assignedTopicPartitions));
    }

    public void OnStop(IImmutableSet<TopicPartition> topicPartitions, IRestrictedConsumer consumer)
    {
        if(topicPartitions.Count == 0) 
            return;
        log.Info("Partitions stopped: {0}", string.Join(", ", topicPartitions));
    }
}