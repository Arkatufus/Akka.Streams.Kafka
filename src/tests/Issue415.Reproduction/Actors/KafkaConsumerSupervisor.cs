using Akka.Actor;
using Akka.Util.Internal;

namespace Issue415.Reproduction.Actors;

public class KafkaConsumerSupervisor<T>: ReceiveActor
{
    private static readonly AtomicCounter WorkerId = new ();
    public static Props Props(KafkaStreamFactory<T> factory, int partitions)
        => Akka.Actor.Props.Create(() => new KafkaConsumerSupervisor<T>(factory, partitions));
    
    private readonly KafkaStreamFactory<T> _factory;
    private readonly int _partitions;
    private readonly IActorRef[] _children;
    private readonly ILoggingAdapter _log;

    public KafkaConsumerSupervisor(KafkaStreamFactory<T> factory, int partitions)
    {
        _factory = factory;
        _partitions = partitions;
        _children = new IActorRef[partitions];
        _log = Context.GetLogger();

        Receive<ChildTerminationInfo>(msg =>
        {
            _log.Warning("Child {0} terminated, restarting.", msg.Name);
            Context.Unwatch(msg.Child);
            CreateWorker(msg.Index);
        });
    }

    protected override void PreRestart(Exception reason, object message)
    {
        _log.Warning(reason, "ConsumerSupervisor restarted. Message: {0}", message);
        base.PreRestart(reason, message);
    }

    protected override void PreStart()
    {
        base.PreStart();
        for (var i = 0; i < _partitions; i++)
        {
            CreateWorker(i);
        }
    }

    private void CreateWorker(int index)
    {
        var id = WorkerId.IncrementAndGet();
        var name = $"worker-{id}";
        var child = Context.ActorOf(ConsumerWorkerActor<T>.Props(_factory), name);
        _children[index] = child;
        Context.WatchWith(child, new ChildTerminationInfo(child, index, name));
    }
}

internal sealed class ChildTerminationInfo
{
    public ChildTerminationInfo(IActorRef child, int index, string name)
    {
        Child = child;
        Index = index;
        Name = name;
    }

    public IActorRef Child { get; }
    public int Index { get; }
    public string Name { get; }
}