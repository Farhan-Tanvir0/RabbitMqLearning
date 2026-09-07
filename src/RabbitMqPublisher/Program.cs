using RabbitMQ.Client;
using System.Text;

internal class Program
{
    private static async Task Main(string[] args)
    {
        Console.WriteLine("RabbitMQ Producer");

        var factory = new ConnectionFactory
        {
            HostName = "localhost"
        };

        using var connection = await factory.CreateConnectionAsync();

        using var channel = await connection.CreateChannelAsync();

        await channel.QueueDeclareAsync(
            queue: "hello",
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: null);

        string message = "Hello RabbitMQ";

        var body = Encoding.UTF8.GetBytes(message);

        await channel.BasicPublishAsync(
            exchange: "",
            routingKey: "hello",
            mandatory: false,
            basicProperties: new BasicProperties(),
            body: body);

        Console.WriteLine("Message Sent");
    }
}