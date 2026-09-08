using RabbitMQ.Client;
using RabbitMQ.Client.Events;

internal class Program
{
    public static async Task Main(string[] args)
    {
        Console.WriteLine("RabbitMQ Payments Consumer");

        var factory = new ConnectionFactory
        {
            HostName = "localhost"
        };

        await using var connection = await factory.CreateConnectionAsync();
        await using var channel = await connection.CreateChannelAsync();

        //this line is omnipotent, it will create the exchange if it doesn't exist, or do nothing if it does.
        await channel.ExchangeDeclareAsync(exchange: "myRoutingExchange", type: ExchangeType.Direct, durable: true, autoDelete: false);

        //this is a temporary queue that will be deleted when the consumer disconnects
        var tempQueue = await channel.QueueDeclareAsync(
            queue: "",
            durable: false,
            exclusive: true,
            autoDelete: true,
            arguments: null);

        await channel.QueueBindAsync(
            queue: tempQueue.QueueName,
            exchange: "myRoutingExchange",
            routingKey: "paymentsKey");

        var consumer = new AsyncEventingBasicConsumer(channel);

        consumer.ReceivedAsync += async (model, ea) =>
        {
            var body = ea.Body.ToArray();
            var message = System.Text.Encoding.UTF8.GetString(body);
            Console.WriteLine($"Received by Payments Consumer: {message}");
            await Task.CompletedTask;
        };

        await channel.BasicConsumeAsync(
            queue: tempQueue.QueueName,
            autoAck: true,
            consumer: consumer);

        Console.WriteLine("Waiting for messages...");
        Console.ReadLine();
    }
}