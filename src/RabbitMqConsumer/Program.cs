using RabbitMQ.Client;
using RabbitMQ.Client.Events;

internal class Program
{
    public static async Task Main(string[] args)
    {
        Console.WriteLine("RabbitMQ Consumer 1");

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

        var consumer = new AsyncEventingBasicConsumer(channel);

        consumer.ReceivedAsync += async (model, ea) =>
        {
            var body = ea.Body.ToArray();
            var message = System.Text.Encoding.UTF8.GetString(body);
            Console.WriteLine($"Received: {message}");
            await Task.CompletedTask;
        };

        await channel.BasicConsumeAsync(
            queue: "hello",
            autoAck: true,
            consumer: consumer);

        Console.WriteLine("Waiting for messages...");
        Console.ReadLine();
    }
}