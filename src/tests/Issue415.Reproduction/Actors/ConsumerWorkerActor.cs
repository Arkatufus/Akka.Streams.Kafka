using Akka.Actor;
using Akka.Streams;
using Akka.Streams.Kafka.Messages;
using Confluent.Kafka;
using Issue415.Reproduction.Proto;

namespace Issue415.Reproduction.Actors
{
    public class ConsumerWorkerActor<T>: ReceiveActor
    {
        public static Props Props(KafkaStreamFactory<T> factory)
            => Akka.Actor.Props.Create(() => new ConsumerWorkerActor<T>(factory));
        
        private readonly ILoggingAdapter _log;
        private readonly KafkaStreamFactory<T> _factory;
        private IKillSwitch? _killSwitch;
        
        public ConsumerWorkerActor(KafkaStreamFactory<T> factory)
        {
            _factory = factory;
            _log = Context.GetLogger();

            Receive<CommittableMessage<Ignore, byte[]>>(msg =>
            {
                var record = msg.Record;
                var model = Model.FromProto(ModelProto.Parser.ParseFrom(record.Message.Value));
                if(_log.IsDebugEnabled)
                    _log.Debug($"{record.Topic}[{record.Partition}][{record.Offset}]: {model}");
                Sender.Tell(msg.CommitableOffset);
            });
        }

        protected override void PostRestart(Exception reason)
        {
            _log.Warning(reason, "Worker restarted");
            base.PostRestart(reason);
        }

        protected override void PreStart()
        {
            base.PreStart();
            var self = Self;
            _killSwitch = _factory.CreateKillableStream(Context, async msg => await self.Ask<ICommittable>(msg));
            _log.Info("Worker started");
        }

        protected override void PostStop()
        {
            base.PostStop();
            _log.Info("Worker stopped");
        }
    }    
}