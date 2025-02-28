using Akka.Actor;
using Akka.Hosting;
using Akka.Streams.Kafka.Settings;
using Confluent.Kafka;
using Issue415.Reproduction.Actors;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Testcontainers.Kafka;
using LogLevel = Microsoft.Extensions.Logging.LogLevel;

namespace Issue415.Reproduction;

public static class Consumer
{
    public static IHost Create(string[] args, KafkaContainer fixture, int groupId, int topicCount, int timeoutMs, string? topicPostfix = null)
    {
        return Host.CreateDefaultBuilder(args)
            .ConfigureLogging(logger =>
            {
                logger.ClearProviders();
                logger.AddConsole();
                logger.Services.Configure<LoggerFilterOptions>(opt =>
                {
                    opt.MinLevel = LogLevel.Information;
                });
            })
            .ConfigureServices((ctx, services) =>
            {
                services
                    .AddAkka("ProducerSys", (builder, provider) =>
                    {
                        builder
                            .ConfigureLoggers(logger =>
                            {
                                logger.ClearLoggers();
                                logger.AddLoggerFactory();
                                logger.LogLevel = Akka.Event.LogLevel.DebugLevel;
                            })
                            .AddHocon(KafkaExtensions.DefaultSettings, HoconAddMode.Append)
                            .AddStartup((system, registry) =>
                            {
                                var bootstrapServer = fixture.GetBootstrapAddress();
                                var loggerFactory = provider.GetRequiredService<ILoggerFactory>();
                                var logger = loggerFactory.CreateLogger(nameof(AkkaConfigurationBuilder));
                                logger.LogInformation("[Consumer] BootstrapServers: {BootstrapServers}", bootstrapServer);
                                logger.LogInformation("[Consumer] GroupId: {GroupId}", groupId);
                                logger.LogInformation("[Consumer] TimeoutMs: {TimeoutMs}", timeoutMs);
                                logger.LogInformation("[Consumer] TopicCount: {TopicCount}", topicCount);
                                logger.LogInformation("[Consumer] TopicPostfix: {TopicPostfix}", topicPostfix);

                                var consumerSettings = ConsumerSettings<Ignore, byte[]>.Create(system, null, null)
                                    .WithBootstrapServers(bootstrapServer)
                                    .WithGroupId(groupId.ToString())
                                    .WithProperty("session.timeout.ms", timeoutMs.ToString());

                                foreach (var topicIndex in Enumerable.Range(1, topicCount))
                                {
                                    var topic = string.IsNullOrWhiteSpace(topicPostfix)
                                        ? $"unexpected-records-{(topicIndex * 100).ToString()}"
                                        : $"unexpected-records-{(topicIndex * 100).ToString()}-{topicPostfix}";
                                    CreateConsumer(system, topic, consumerSettings);
                                }
                            });
                    });
            })
            .UseConsoleLifetime()
            .Build();
    }
    
    private static IActorRef CreateConsumer(
        ActorSystem system,
        string topic,
        ConsumerSettings<Ignore, byte[]> consumerSettings)
    {
        var logger = Logging.GetLogger(system, $"kafka-{topic}-source");
        var eventLogger = Logging.GetLogger(system, $"kafka-{topic}-partition-events");
        
        var factory = new KafkaStreamFactory<Model>(
            new KafkaSourceDecider(logger, true), 
            topic,
            consumerSettings,
            new CustomPartitionEventHandler(eventLogger));
            
        return system.ActorOf(KafkaConsumerSupervisor<Model>.Props(factory, 1), $"kafka-{topic}-actor");
    }
    
}