
//This Client is Desined to understant Computing Consumer Pattern,
//where the messages are distributed among the consumers,
//and each consumer will process one message at a time.
//This is achieved by setting the prefetch count to 1,
//which means that the consumer will only receive one message at a time,
//and it will not receive another message until it has acknowledged the previous one.

using RabbitMQ.Client;
using RabbitMQ.Client.Events;

internal class Program
{
    public static async Task Main(string[] args)
    {
        Console.WriteLine("RabbitMQ Consumer 2");

        var factory = new ConnectionFactory
        {
            HostName = "localhost"
        };

        await using var connection = await factory.CreateConnectionAsync();
        await using var channel = await connection.CreateChannelAsync();

        await channel.QueueDeclareAsync(
            queue: "hello",
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: null);

        await channel.BasicQosAsync(
            prefetchSize: 0,
            prefetchCount: 1,
            global: false);

        var consumer = new AsyncEventingBasicConsumer(channel);

        var random = new Random();

        consumer.ReceivedAsync += async (model, ea) =>
        {
            var delay = random.Next(1, 5);

            var body = ea.Body.ToArray();
            var message = System.Text.Encoding.UTF8.GetString(body);
            Console.WriteLine($"Received: {message} will Take {delay}s");

            await Task.Delay(TimeSpan.FromSeconds(delay));

            await channel.BasicAckAsync(deliveryTag: ea.DeliveryTag, multiple: false);
        };

        await channel.BasicConsumeAsync(
            queue: "hello",
            autoAck: false,
            consumer: consumer);

        Console.WriteLine("Waiting for messages...");
        Console.ReadLine();
    }
}