using Issue415.Reproduction;
using Microsoft.Extensions.Hosting;
using Testcontainers.Kafka;

Console.WriteLine("Starting Kafka");
var container = new KafkaBuilder()
    .WithImage("confluentinc/cp-kafka:7.9.0")
    .WithEnvironment(new Dictionary<string, string>
    {
        ["KAFKA_BROKER_ID"] = "1",
        ["KAFKA_NUM_PARTITIONS"] = "3",
        ["KAFKA_AUTO_CREATE_TOPICS_ENABLE"] = "true",
        ["KAFKA_DELETE_TOPIC_ENABLE"] = "true",
        ["KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR"] = "1",
    })
    .Build();

await container.StartAsync();
Console.WriteLine("Kafka started");

var consumers = new IHost?[3];
try
{
    // Start producer
    var producerTask = Producer.Create(args, container, 3, TimeSpan.FromMilliseconds(200));

    consumers[0] = Consumer.Create(args, container, 1, 3, 6000);
    consumers[1] = Consumer.Create(args, container, 1, 3, 6000);
    consumers[2] = Consumer.Create(args, container, 1, 3, 6000);
    
    await consumers[0]!.StartAsync();
    await consumers[1]!.StartAsync();
    await consumers[2]!.StartAsync();
    
    var timer = new PeriodicTimer(TimeSpan.FromSeconds(1));
    
    while (await timer.WaitForNextTickAsync())
    {
        if (consumers[2] is not null)
        {
            await consumers[2]!.StopAsync();
            consumers[2]!.Dispose();
            consumers[2] = null;
        }
        else
        {
            consumers[2] = Consumer.Create(args, container, 1, 3, 6000);
            await consumers[2]!.StartAsync();
        }
    }
}
finally
{
    await container.StopAsync();
    await container.DisposeAsync();
    Console.WriteLine("Kafka disposed");
}
