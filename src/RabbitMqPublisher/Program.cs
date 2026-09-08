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

        //await channel.ExchangeDeclareAsync(exchange: "PubSub", type: ExchangeType.Fanout, durable: true, autoDelete: false);
        //await channel.ExchangeDeclareAsync(exchange: "myRoutingExchange", type: ExchangeType.Direct, durable: true, autoDelete: false);
        await channel.ExchangeDeclareAsync(exchange: "myTopicExchange", type: ExchangeType.Topic, durable: true, autoDelete: false);

        //await channel.QueueDeclareAsync(
        //    queue: "hello",
        //    durable: true,
        //    exclusive: false,
        //    autoDelete: false,
        //    arguments: null);


        //var random = new Random();
        //int messageId = 1;

        //while (true)
        //{
        //    var publishiingDelay = random.Next(1, 3);

        //    string message = $"Message {messageId}";

        //    var body = Encoding.UTF8.GetBytes(message);

        //    await channel.BasicPublishAsync(
        //        exchange: "",
        //        routingKey: "hello",
        //        mandatory: false,
        //        basicProperties: new BasicProperties(),
        //        body: body);

        //    Console.WriteLine($"Message Sent: {message}");
        //    messageId++;
        //    await Task.Delay(TimeSpan.FromSeconds(publishiingDelay));
        //}

        string message = $"Message For  Analytics , User and Payments Consumers";

        var body = Encoding.UTF8.GetBytes(message);

        await channel.BasicPublishAsync(
            exchange: "myTopicExchange",
            routingKey: "user.Bangladesh.payments",
            mandatory: false,
            basicProperties: new BasicProperties(),
            body: body);


        string message2 = $"Message For  only Analytics Consumer";

        var body2 = Encoding.UTF8.GetBytes(message2);

        await channel.BasicPublishAsync(
            exchange: "myTopicExchange",
            routingKey: "Data.Bangladesh.analytics",
            mandatory: false,
            basicProperties: new BasicProperties(),
            body: body2);

        Console.WriteLine($"Message Sent: {message}");
        Console.WriteLine($"Message Sent: {message2}");
    }
}