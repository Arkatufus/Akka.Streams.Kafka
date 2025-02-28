using Akka;
using Akka.Hosting;
using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.Streams.Kafka.Dsl;
using Akka.Streams.Kafka.Messages;
using Akka.Streams.Kafka.Settings;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Google.Protobuf;
using Issue415.Reproduction.Proto;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Testcontainers.Kafka;
using LogLevel = Microsoft.Extensions.Logging.LogLevel;

namespace Issue415.Reproduction;

public static class Producer
{
    public static async Task Create(
        string[] args,
        KafkaContainer fixture,
        int topicCount,
        TimeSpan produceDelay,
        CancellationTokenSource cts,
        string? topicPostfix = null)
    {
        topicPostfix ??= string.Empty;
        
        await Host.CreateDefaultBuilder(args)
            .ConfigureLogging(logger =>
            {
                //logger.ClearProviders();
                //logger.AddConsole();
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
                            .AddStartup(async (system, registry) =>
                            {
                                var bootstrapServer = fixture.GetBootstrapAddress();
                                var loggerFactory = provider.GetRequiredService<ILoggerFactory>();
                                var log = loggerFactory.CreateLogger<AkkaConfigurationBuilder>();
                                log.LogInformation("[Producer] BootstrapServers: {BootstrapServers}", bootstrapServer);
                                log.LogInformation("[Producer] TopicCount: {TopicCount}", topicCount);
                                log.LogInformation("[Producer] ProduceDelayMs: {ProduceDelayMs}", produceDelay);
                                log.LogInformation("[Producer] TopicPostfix: {TopicPostfix}", topicPostfix);

                                var materializer = system.Materializer();

                                var adminConfig = new AdminClientConfig
                                {
                                    BootstrapServers = bootstrapServer
                                };

                                var producerSettings = ProducerSettings<Null, byte[]>.Create(system, null, null)
                                    .WithBootstrapServers(bootstrapServer);

                                var tasks = Enumerable.Range(1, topicCount)
                                    .Select(topicIndex =>
                                        {
                                            var topic = string.IsNullOrWhiteSpace(topicPostfix)
                                                ? $"topic-{(topicIndex * 100).ToString()}"
                                                : $"topic-{(topicIndex * 100).ToString()}-{topicPostfix}";
                                            var logger = Logging.GetLogger(system, $"{topic}-producer");
                                            return CreateProducerAsync(
                                                topic: topic,
                                                materializer: materializer,
                                                adminConfig: adminConfig,
                                                producerSettings: producerSettings,
                                                log: logger,
                                                delay: produceDelay);
                                        }
                                    );

                                await Task.WhenAll(tasks);
                            });
                    });
            })
            .UseConsoleLifetime()
            .RunConsoleAsync();
        
        await cts.CancelAsync();
    }

    private static async Task CreateProducerAsync(
        string topic,
        ActorMaterializer materializer,
        AdminClientConfig adminConfig,
        ProducerSettings<Null, byte[]> producerSettings,
        ILoggingAdapter log,
        TimeSpan delay)
    {
        using (var adminClient = new AdminClientBuilder(adminConfig).Build())
        {
            try
            {
                const int numPartitions = 6;
                const short replicationFactor = 1; 

                // Create a new topic with defined partitions
                await adminClient.CreateTopicsAsync([
                    new TopicSpecification
                    {
                        Name = topic,
                        NumPartitions = numPartitions,
                        ReplicationFactor = replicationFactor
                    }
                ]);

                log.Info($"Topic '{topic}' created with {numPartitions} partitions.");
            }
            catch (CreateTopicsException e)
            {
                log.Warning($"An error occurred creating the topic: {e.Results[0].Error.Reason}");
            }
        }
        
        // Deliberately run a detached task that runs forever
        _ = Source
            .Cycle(() => Enumerable.Range(1, 1000).GetEnumerator())
            .Throttle(1, delay, 1, ThrottleMode.Shaping)
            .Select(c => new Model(c, c.ToString(), DateTime.UtcNow).ToProto().ToByteArray())
            .Select(elem => ProducerMessage.Single(new ProducerRecord<Null, byte[]>(topic, null!, elem)))
            .Via(KafkaProducer.FlexiFlow<Null, byte[], NotUsed>(producerSettings))
            .Select(result =>
            {
                var response = (Result<Null, byte[], NotUsed>)result;
                var meta = response.Metadata;
                
                if(log.IsDebugEnabled)
                {
                    var value = Model.FromProto(ModelProto.Parser.ParseFrom(meta.Value));  
                    log.Debug("Producer: {0}/{1} {2}: {3}", meta.Topic, meta.Partition, meta.Offset, value.Sequence);
                }
                return result;
            })
            .RunWith(Sink.Ignore<IResults<Null, byte[], NotUsed>>(), materializer);
    }
}