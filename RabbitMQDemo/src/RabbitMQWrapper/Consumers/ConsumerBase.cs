using System.Text;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQWrapper.Connection;
using RabbitMQWrapper.Models;
using RabbitMQWrapper.Options;

namespace RabbitMQWrapper.Consumers;

/// <summary>
/// 基础消费者接口
/// </summary>
public interface IConsumer
{
    /// <summary>
    /// 启动消费者
    /// </summary>
    void Start();

    /// <summary>
    /// 停止消费者
    /// </summary>
    void Stop();
}

/// <summary>
/// 泛型消费者接口
/// </summary>
/// <typeparam name="T">消息类型</typeparam>
public interface IConsumer<T> : IConsumer where T : class
{
    /// <summary>
    /// 消息处理函数
    /// </summary>
    Func<MessageContext<T>, Task<MessageProcessResult>>? OnMessageReceived { get; set; }
}

/// <summary>
/// 消费者基类
/// 提供通用的消费者功能：队列声明、QoS设置、消息确认、重试机制等
/// </summary>
/// <typeparam name="T">消息类型</typeparam>
public abstract class ConsumerBase<T> : IConsumer<T> where T : class
{
    protected readonly IRabbitMQConnectionManager ConnectionManager;
    protected readonly ConsumerOptions Options;
    protected readonly ILogger Logger;
    protected IModel? Channel;
    protected EventingBasicConsumer? Consumer;
    protected string? ConsumerTag;
    protected bool IsRunning;

    /// <summary>
    /// 消息处理函数
    /// </summary>
    public Func<MessageContext<T>, Task<MessageProcessResult>>? OnMessageReceived { get; set; }

    /// <summary>
    /// 构造函数
    /// </summary>
    /// <param name="connectionManager">连接管理器</param>
    /// <param name="options">消费者配置</param>
    /// <param name="logger">日志记录器</param>
    protected ConsumerBase(IRabbitMQConnectionManager connectionManager, ConsumerOptions options, ILogger logger)
    {
        ConnectionManager = connectionManager;
        Options = options;
        Logger = logger;
    }

    /// <summary>
    /// 启动消费者
    /// </summary>
    public virtual void Start()
    {
        if (IsRunning) return;

        ConnectionManager.EnsureConnected();
        Channel = ConnectionManager.CreateChannel();

        DeclareQueue(Channel);
        Channel.BasicQos(prefetchSize: 0, prefetchCount: Options.PrefetchCount, global: false);

        Consumer = new EventingBasicConsumer(Channel);
        Consumer.Received += OnConsumerReceived;

        ConsumerTag = Channel.BasicConsume(
            queue: Options.QueueName,
            autoAck: Options.AutoAck,
            consumerTag: Options.ConsumerTag ?? string.Empty,
            consumer: Consumer);

        IsRunning = true;
        Logger.LogInformation("消费者已启动，队列: {QueueName}, 消费者标签: {ConsumerTag}", Options.QueueName, ConsumerTag);
    }

    /// <summary>
    /// 停止消费者
    /// </summary>
    public virtual void Stop()
    {
        if (!IsRunning) return;

        try
        {
            if (Channel != null && ConsumerTag != null)
            {
                Channel.BasicCancel(ConsumerTag);
            }

            if (Consumer != null)
            {
                Consumer.Received -= OnConsumerReceived;
            }

            Channel?.Close();
            Channel?.Dispose();
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "停止消费者时发生异常");
        }
        finally
        {
            IsRunning = false;
            Logger.LogInformation("消费者已停止，队列: {QueueName}", Options.QueueName);
        }
    }

    /// <summary>
    /// 声明队列
    /// 可被子类重写以添加特殊配置（如死信队列参数）
    /// </summary>
    /// <param name="channel">通道</param>
    protected virtual void DeclareQueue(IModel channel)
    {
        channel.QueueDeclare(
            queue: Options.QueueName,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: null);
    }

    /// <summary>
    /// 消息接收处理
    /// </summary>
    protected virtual async void OnConsumerReceived(object? sender, BasicDeliverEventArgs e)
    {
        var messageId = e.BasicProperties?.MessageId ?? string.Empty;
        var retryCount = GetRetryCount(e.BasicProperties?.Headers);

        try
        {
            var body = e.Body.ToArray();
            var message = DeserializeMessage(body);

            var context = new MessageContext<T>
            {
                Body = message,
                MessageId = messageId,
                RoutingKey = e.RoutingKey,
                Exchange = e.Exchange,
                RetryCount = retryCount,
                DeliveryTag = e.DeliveryTag,
                Headers = e.BasicProperties?.Headers
            };

            Logger.LogDebug("收到消息, MessageId: {MessageId}, 重试次数: {RetryCount}", messageId, retryCount);

            if (OnMessageReceived == null)
            {
                Logger.LogWarning("未设置消息处理函数，直接确认消息, MessageId: {MessageId}", messageId);
                AckMessage(e.DeliveryTag);
                return;
            }

            var result = await OnMessageReceived(context);

            switch (result)
            {
                case MessageProcessResult.Success:
                    AckMessage(e.DeliveryTag);
                    Logger.LogDebug("消息处理成功，已确认, MessageId: {MessageId}", messageId);
                    break;

                case MessageProcessResult.Retry:
                    await HandleRetry(e, messageId, retryCount);
                    break;

                case MessageProcessResult.Reject:
                    HandleReject(e, messageId);
                    break;
            }
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "处理消息时发生异常, MessageId: {MessageId}", messageId);
            await HandleRetry(e, messageId, retryCount);
        }
    }

    /// <summary>
    /// 处理重试
    /// 未达到最大重试次数则重新入队，否则进入死信队列或丢弃
    /// </summary>
    protected virtual async Task HandleRetry(BasicDeliverEventArgs e, string messageId, int retryCount)
    {
        if (retryCount < Options.MaxRetryCount)
        {
            Logger.LogInformation("消息重试中, MessageId: {MessageId}, 当前重试次数: {RetryCount}/{MaxRetryCount}",
                messageId, retryCount, Options.MaxRetryCount);

            if (Options.RetryInterval > 0)
            {
                await Task.Delay(Options.RetryInterval);
            }

            NackMessage(e.DeliveryTag, requeue: true);
        }
        else
        {
            Logger.LogWarning("消息达到最大重试次数, MessageId: {MessageId}, MaxRetryCount: {MaxRetryCount}",
                messageId, Options.MaxRetryCount);
            HandleReject(e, messageId);
        }
    }

    /// <summary>
    /// 处理拒绝消息
    /// 如果启用死信队列则进入死信队列，否则丢弃
    /// </summary>
    protected virtual void HandleReject(BasicDeliverEventArgs e, string messageId)
    {
        Logger.LogWarning("消息被拒绝, MessageId: {MessageId}", messageId);
        NackMessage(e.DeliveryTag, requeue: false);
    }

    /// <summary>
    /// 确认消息
    /// </summary>
    protected void AckMessage(ulong deliveryTag)
    {
        if (!Options.AutoAck && Channel != null && Channel.IsOpen)
        {
            Channel.BasicAck(deliveryTag, multiple: false);
        }
    }

    /// <summary>
    /// 拒绝消息
    /// </summary>
    protected void NackMessage(ulong deliveryTag, bool requeue)
    {
        if (!Options.AutoAck && Channel != null && Channel.IsOpen)
        {
            Channel.BasicNack(deliveryTag, multiple: false, requeue: requeue);
        }
    }

    /// <summary>
    /// 获取重试次数
    /// 从消息头中读取重试次数
    /// </summary>
    protected int GetRetryCount(IDictionary<string, object>? headers)
    {
        if (headers == null || !headers.ContainsKey("x-retry-count"))
            return 0;

        if (headers["x-retry-count"] is byte[] bytes)
        {
            var countStr = Encoding.UTF8.GetString(bytes);
            if (int.TryParse(countStr, out var count))
                return count;
        }

        return 0;
    }

    /// <summary>
    /// 反序列化消息
    /// </summary>
    protected virtual T DeserializeMessage(byte[] body)
    {
        var json = Encoding.UTF8.GetString(body);
        var message = JsonConvert.DeserializeObject<T>(json);
        if (message == null)
        {
            throw new InvalidOperationException("消息反序列化失败");
        }
        return message;
    }
}
