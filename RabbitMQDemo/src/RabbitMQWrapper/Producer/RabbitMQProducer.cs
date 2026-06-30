using System.Text;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQWrapper.ChannelPool;
using RabbitMQWrapper.Models;
using RabbitMQWrapper.Options;

namespace RabbitMQWrapper.Producer;

/// <summary>
/// RabbitMQ 生产者接口
/// 支持发布确认、死信队列、延迟消息等高级特性
/// 使用通道池提高并发性能
/// </summary>
public interface IRabbitMQProducer
{
    /// <summary>
    /// 发布消息（简单模式）
    /// </summary>
    /// <typeparam name="T">消息类型</typeparam>
    /// <param name="queueName">队列名称</param>
    /// <param name="message">消息内容</param>
    /// <returns>发布结果</returns>
    PublishResult Publish<T>(string queueName, T message) where T : class;

    /// <summary>
    /// 发布消息（交换机模式）
    /// </summary>
    /// <typeparam name="T">消息类型</typeparam>
    /// <param name="exchangeName">交换机名称</param>
    /// <param name="routingKey">路由键</param>
    /// <param name="message">消息内容</param>
    /// <param name="exchangeType">交换机类型，默认 direct</param>
    /// <returns>发布结果</returns>
    PublishResult PublishToExchange<T>(string exchangeName, string routingKey, T message, string exchangeType = "direct") where T : class;

    /// <summary>
    /// 发布消息（带死信队列）
    /// </summary>
    /// <typeparam name="T">消息类型</typeparam>
    /// <param name="queueName">队列名称</param>
    /// <param name="message">消息内容</param>
    /// <param name="deadLetterOptions">死信队列配置</param>
    /// <returns>发布结果</returns>
    PublishResult PublishWithDeadLetter<T>(string queueName, T message, DeadLetterOptions deadLetterOptions) where T : class;

    /// <summary>
    /// 发布延迟消息（通过死信队列实现）
    /// </summary>
    /// <typeparam name="T">消息类型</typeparam>
    /// <param name="queueName">目标队列名称</param>
    /// <param name="message">消息内容</param>
    /// <param name="delayMilliseconds">延迟时间（毫秒）</param>
    /// <returns>发布结果</returns>
    PublishResult PublishDelayed<T>(string queueName, T message, int delayMilliseconds) where T : class;

    /// <summary>
    /// 批量发布消息
    /// </summary>
    /// <typeparam name="T">消息类型</typeparam>
    /// <param name="queueName">队列名称</param>
    /// <param name="messages">消息列表</param>
    /// <returns>发布结果列表</returns>
    List<PublishResult> BatchPublish<T>(string queueName, IEnumerable<T> messages) where T : class;
}

/// <summary>
/// RabbitMQ 生产者实现
/// 支持发布确认机制确保消息可靠投递
/// 使用通道池管理通道，提高高并发性能
/// </summary>
public class RabbitMQProducer : IRabbitMQProducer
{
    private readonly IChannelPool _channelPool;
    private readonly RabbitMQOptions _options;
    private readonly ILogger<RabbitMQProducer> _logger;

    /// <summary>
    /// 构造函数
    /// </summary>
    /// <param name="channelPool">通道池</param>
    /// <param name="options">RabbitMQ 配置</param>
    /// <param name="logger">日志记录器</param>
    public RabbitMQProducer(IChannelPool channelPool, IOptions<RabbitMQOptions> options, ILogger<RabbitMQProducer> logger)
    {
        _channelPool = channelPool;
        _options = options.Value;
        _logger = logger;
    }

    /// <summary>
    /// 发布消息（简单模式）
    /// 直接发送到指定队列
    /// </summary>
    /// <typeparam name="T">消息类型</typeparam>
    /// <param name="queueName">队列名称</param>
    /// <param name="message">消息内容</param>
    /// <returns>发布结果</returns>
    public PublishResult Publish<T>(string queueName, T message) where T : class
    {
        if (string.IsNullOrWhiteSpace(queueName))
            throw new ArgumentException("队列名称不能为空", nameof(queueName));
        if (message == null)
            throw new ArgumentNullException(nameof(message));

        return _channelPool.UseChannel(channel =>
        {
            try
            {
                channel.QueueDeclare(queue: queueName, durable: true, exclusive: false, autoDelete: false, arguments: null);

                var messageId = Guid.NewGuid().ToString();
                var body = SerializeMessage(message);
                var properties = CreateBasicProperties(channel, messageId);

                if (_options.PublisherConfirmsEnabled)
                {
                    return PublishWithConfirm(channel, string.Empty, queueName, properties, body, messageId);
                }

                channel.BasicPublish(exchange: string.Empty, routingKey: queueName, basicProperties: properties, body: body);
                _logger.LogDebug("消息已发布到队列 {QueueName}, MessageId: {MessageId}", queueName, messageId);
                return PublishResult.Ok(messageId);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "发布消息到队列 {QueueName} 失败: {Message}", queueName, ex.Message);
                return PublishResult.Fail(ex.Message);
            }
        });
    }

    /// <summary>
    /// 发布消息（交换机模式）
    /// 通过交换机和路由键发送消息
    /// </summary>
    /// <typeparam name="T">消息类型</typeparam>
    /// <param name="exchangeName">交换机名称</param>
    /// <param name="routingKey">路由键</param>
    /// <param name="message">消息内容</param>
    /// <param name="exchangeType">交换机类型</param>
    /// <returns>发布结果</returns>
    public PublishResult PublishToExchange<T>(string exchangeName, string routingKey, T message, string exchangeType = "direct") where T : class
    {
        if (string.IsNullOrWhiteSpace(exchangeName))
            throw new ArgumentException("交换机名称不能为空", nameof(exchangeName));
        if (message == null)
            throw new ArgumentNullException(nameof(message));

        return _channelPool.UseChannel(channel =>
        {
            try
            {
                channel.ExchangeDeclare(exchange: exchangeName, type: exchangeType, durable: true, autoDelete: false, arguments: null);

                var messageId = Guid.NewGuid().ToString();
                var body = SerializeMessage(message);
                var properties = CreateBasicProperties(channel, messageId);

                if (_options.PublisherConfirmsEnabled)
                {
                    return PublishWithConfirm(channel, exchangeName, routingKey, properties, body, messageId);
                }

                channel.BasicPublish(exchange: exchangeName, routingKey: routingKey, basicProperties: properties, body: body);
                _logger.LogDebug("消息已发布到交换机 {ExchangeName}, 路由键: {RoutingKey}, MessageId: {MessageId}",
                    exchangeName, routingKey, messageId);
                return PublishResult.Ok(messageId);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "发布消息到交换机 {ExchangeName} 失败: {Message}", exchangeName, ex.Message);
                return PublishResult.Fail(ex.Message);
            }
        });
    }

    /// <summary>
    /// 发布消息（带死信队列）
    /// 自动声明队列和死信交换机/队列
    /// </summary>
    /// <typeparam name="T">消息类型</typeparam>
    /// <param name="queueName">队列名称</param>
    /// <param name="message">消息内容</param>
    /// <param name="deadLetterOptions">死信队列配置</param>
    /// <returns>发布结果</returns>
    public PublishResult PublishWithDeadLetter<T>(string queueName, T message, DeadLetterOptions deadLetterOptions) where T : class
    {
        if (string.IsNullOrWhiteSpace(queueName))
            throw new ArgumentException("队列名称不能为空", nameof(queueName));
        if (message == null)
            throw new ArgumentNullException(nameof(message));
        if (deadLetterOptions == null)
            throw new ArgumentNullException(nameof(deadLetterOptions));

        return _channelPool.UseChannel(channel =>
        {
            try
            {
                var dlxName = deadLetterOptions.DeadLetterExchange ?? $"{queueName}.dlx";
                var dlRoutingKey = deadLetterOptions.DeadLetterRoutingKey ?? $"{queueName}.deadletter";
                var dlQueueName = deadLetterOptions.DeadLetterQueueName ?? $"{queueName}.deadletter";

                channel.ExchangeDeclare(exchange: dlxName, type: "direct", durable: true, autoDelete: false, arguments: null);
                channel.QueueDeclare(queue: dlQueueName, durable: true, exclusive: false, autoDelete: false, arguments: null);
                channel.QueueBind(queue: dlQueueName, exchange: dlxName, routingKey: dlRoutingKey);

                var queueArgs = new Dictionary<string, object>
                {
                    { "x-dead-letter-exchange", dlxName },
                    { "x-dead-letter-routing-key", dlRoutingKey }
                };

                if (deadLetterOptions.MessageTtl.HasValue)
                {
                    queueArgs.Add("x-message-ttl", deadLetterOptions.MessageTtl.Value);
                }

                if (deadLetterOptions.MaxLength.HasValue)
                {
                    queueArgs.Add("x-max-length", deadLetterOptions.MaxLength.Value);
                }

                channel.QueueDeclare(queue: queueName, durable: true, exclusive: false, autoDelete: false, arguments: queueArgs);

                var messageId = Guid.NewGuid().ToString();
                var body = SerializeMessage(message);
                var properties = CreateBasicProperties(channel, messageId);

                if (_options.PublisherConfirmsEnabled)
                {
                    return PublishWithConfirm(channel, string.Empty, queueName, properties, body, messageId);
                }

                channel.BasicPublish(exchange: string.Empty, routingKey: queueName, basicProperties: properties, body: body);
                _logger.LogDebug("消息已发布到队列 {QueueName}(带死信队列), MessageId: {MessageId}", queueName, messageId);
                return PublishResult.Ok(messageId);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "发布消息到队列 {QueueName}(带死信队列) 失败: {Message}", queueName, ex.Message);
                return PublishResult.Fail(ex.Message);
            }
        });
    }

    /// <summary>
    /// 发布延迟消息
    /// 通过死信队列 + TTL 实现延迟消息
    /// 消息先进入延迟队列，过期后通过死信交换机路由到目标队列
    /// </summary>
    /// <typeparam name="T">消息类型</typeparam>
    /// <param name="queueName">目标队列名称</param>
    /// <param name="message">消息内容</param>
    /// <param name="delayMilliseconds">延迟时间（毫秒）</param>
    /// <returns>发布结果</returns>
    public PublishResult PublishDelayed<T>(string queueName, T message, int delayMilliseconds) where T : class
    {
        if (string.IsNullOrWhiteSpace(queueName))
            throw new ArgumentException("队列名称不能为空", nameof(queueName));
        if (message == null)
            throw new ArgumentNullException(nameof(message));
        if (delayMilliseconds <= 0)
            throw new ArgumentException("延迟时间必须大于0", nameof(delayMilliseconds));

        return _channelPool.UseChannel(channel =>
        {
            try
            {
                var delayQueueName = $"{queueName}.delay.{delayMilliseconds}ms";
                var delayExchangeName = $"{queueName}.delay.exchange";

                channel.ExchangeDeclare(exchange: delayExchangeName, type: "direct", durable: true, autoDelete: false, arguments: null);
                channel.QueueDeclare(queue: queueName, durable: true, exclusive: false, autoDelete: false, arguments: null);
                channel.QueueBind(queue: queueName, exchange: delayExchangeName, routingKey: queueName);

                var delayQueueArgs = new Dictionary<string, object>
                {
                    { "x-dead-letter-exchange", delayExchangeName },
                    { "x-dead-letter-routing-key", queueName },
                    { "x-message-ttl", delayMilliseconds }
                };

                channel.QueueDeclare(queue: delayQueueName, durable: true, exclusive: false, autoDelete: false, arguments: delayQueueArgs);

                var messageId = Guid.NewGuid().ToString();
                var body = SerializeMessage(message);
                var properties = CreateBasicProperties(channel, messageId);
                properties.Expiration = delayMilliseconds.ToString();

                if (_options.PublisherConfirmsEnabled)
                {
                    return PublishWithConfirm(channel, string.Empty, delayQueueName, properties, body, messageId);
                }

                channel.BasicPublish(exchange: string.Empty, routingKey: delayQueueName, basicProperties: properties, body: body);
                _logger.LogDebug("延迟消息已发布, 目标队列: {QueueName}, 延迟: {Delay}ms, MessageId: {MessageId}",
                    queueName, delayMilliseconds, messageId);
                return PublishResult.Ok(messageId);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "发布延迟消息到队列 {QueueName} 失败: {Message}", queueName, ex.Message);
                return PublishResult.Fail(ex.Message);
            }
        });
    }

    /// <summary>
    /// 批量发布消息
    /// 适合需要发送大量消息的场景
    /// </summary>
    /// <typeparam name="T">消息类型</typeparam>
    /// <param name="queueName">队列名称</param>
    /// <param name="messages">消息列表</param>
    /// <returns>发布结果列表</returns>
    public List<PublishResult> BatchPublish<T>(string queueName, IEnumerable<T> messages) where T : class
    {
        if (string.IsNullOrWhiteSpace(queueName))
            throw new ArgumentException("队列名称不能为空", nameof(queueName));
        if (messages == null)
            throw new ArgumentNullException(nameof(messages));

        var results = new List<PublishResult>();

        return _channelPool.UseChannel(channel =>
        {
            try
            {
                channel.QueueDeclare(queue: queueName, durable: true, exclusive: false, autoDelete: false, arguments: null);

                if (_options.PublisherConfirmsEnabled)
                {
                    channel.ConfirmSelect();
                }

                foreach (var message in messages)
                {
                    try
                    {
                        var messageId = Guid.NewGuid().ToString();
                        var body = SerializeMessage(message);
                        var properties = CreateBasicProperties(channel, messageId);

                        channel.BasicPublish(exchange: string.Empty, routingKey: queueName, basicProperties: properties, body: body);
                        results.Add(PublishResult.Ok(messageId));
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "批量发布单条消息失败: {Message}", ex.Message);
                        results.Add(PublishResult.Fail(ex.Message));
                    }
                }

                if (_options.PublisherConfirmsEnabled)
                {
                    var confirmed = channel.WaitForConfirms(TimeSpan.FromMilliseconds(_options.PublisherConfirmTimeout));
                    if (!confirmed)
                    {
                        _logger.LogWarning("批量发布消息确认超时");
                    }
                }

                _logger.LogDebug("批量发布消息完成，总数: {Count}, 成功: {SuccessCount}",
                    results.Count, results.Count(r => r.Success));
                return results;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "批量发布消息到队列 {QueueName} 失败: {Message}", queueName, ex.Message);
                results.Add(PublishResult.Fail(ex.Message));
                return results;
            }
        });
    }

    private PublishResult PublishWithConfirm(IModel channel, string exchange, string routingKey, IBasicProperties properties, byte[] body, string messageId)
    {
        channel.ConfirmSelect();

        var confirmed = new ManualResetEventSlim(false);
        bool isAcked = false;
        bool isNacked = false;

        void BasicAcksHandler(ulong deliveryTag, bool multiple)
        {
            isAcked = true;
            confirmed.Set();
        }

        void BasicNacksHandler(ulong deliveryTag, bool multiple)
        {
            isNacked = true;
            confirmed.Set();
        }

        void BasicReturnHandler(object? sender, BasicReturnEventArgs e)
        {
            isNacked = true;
            confirmed.Set();
        }

        channel.BasicAcks += BasicAcksHandler;
        channel.BasicNacks += BasicNacksHandler;
        channel.BasicReturn += BasicReturnHandler;

        try
        {
            channel.BasicPublish(exchange: exchange, routingKey: routingKey, mandatory: true, basicProperties: properties, body: body);

            var success = confirmed.Wait(TimeSpan.FromMilliseconds(_options.PublisherConfirmTimeout));

            if (success && isAcked)
            {
                _logger.LogDebug("消息发布确认成功, MessageId: {MessageId}", messageId);
                return PublishResult.Ok(messageId);
            }
            else if (isNacked)
            {
                _logger.LogWarning("消息发布被拒绝, MessageId: {MessageId}", messageId);
                return PublishResult.Fail("消息被Broker拒绝");
            }
            else
            {
                _logger.LogWarning("消息发布确认超时, MessageId: {MessageId}", messageId);
                return PublishResult.Fail("发布确认超时");
            }
        }
        finally
        {
            channel.BasicAcks -= BasicAcksHandler;
            channel.BasicNacks -= BasicNacksHandler;
            channel.BasicReturn -= BasicReturnHandler;
            confirmed.Dispose();
        }
    }

    private IBasicProperties CreateBasicProperties(IModel channel, string messageId)
    {
        var properties = channel.CreateBasicProperties();
        properties.MessageId = messageId;
        properties.DeliveryMode = 2;
        properties.ContentType = "application/json";
        properties.Timestamp = new AmqpTimestamp(DateTimeOffset.UtcNow.ToUnixTimeSeconds());
        return properties;
    }

    private byte[] SerializeMessage<T>(T message) where T : class
    {
        var json = JsonConvert.SerializeObject(message);
        return Encoding.UTF8.GetBytes(json);
    }
}
