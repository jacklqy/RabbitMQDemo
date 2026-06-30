using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQWrapper.Connection;
using RabbitMQWrapper.Models;
using RabbitMQWrapper.Options;

namespace RabbitMQWrapper.Consumers;

/// <summary>
/// 基础消费者
/// 适用于简单的单条消息处理场景
/// 支持手动确认、重试机制
/// 使用场景：订单处理、邮件发送、短信通知等
/// </summary>
/// <typeparam name="T">消息类型</typeparam>
public class BasicConsumer<T> : ConsumerBase<T> where T : class
{
    /// <summary>
    /// 构造函数
    /// </summary>
    /// <param name="connectionManager">连接管理器</param>
    /// <param name="options">消费者配置</param>
    /// <param name="logger">日志记录器</param>
    public BasicConsumer(IRabbitMQConnectionManager connectionManager, ConsumerOptions options, ILogger<BasicConsumer<T>> logger)
        : base(connectionManager, options, logger)
    {
    }
}

/// <summary>
/// 批量消费者
/// 适用于需要批量处理消息的场景
/// 收集一定数量或时间的消息后批量处理
/// 使用场景：批量数据导入、批量更新数据库、日志批量写入等
/// </summary>
/// <typeparam name="T">消息类型</typeparam>
public class BatchConsumer<T> : ConsumerBase<T> where T : class
{
    private readonly List<MessageContext<T>> _batchMessages = new();
    private readonly int _batchSize;
    private readonly TimeSpan _batchTimeout;
    private Timer? _batchTimer;
    private readonly object _batchLock = new();

    /// <summary>
    /// 批量消息处理函数
    /// </summary>
    public Func<List<MessageContext<T>>, Task<MessageProcessResult>>? OnBatchMessageReceived { get; set; }

    /// <summary>
    /// 构造函数
    /// </summary>
    /// <param name="connectionManager">连接管理器</param>
    /// <param name="options">消费者配置</param>
    /// <param name="batchSize">批量大小，默认 100</param>
    /// <param name="batchTimeoutMs">批量超时时间（毫秒），默认 5000ms</param>
    /// <param name="logger">日志记录器</param>
    public BatchConsumer(
        IRabbitMQConnectionManager connectionManager,
        ConsumerOptions options,
        ILogger<BatchConsumer<T>> logger,
        int batchSize = 100,
        int batchTimeoutMs = 5000)
        : base(connectionManager, options, logger)
    {
        _batchSize = batchSize;
        _batchTimeout = TimeSpan.FromMilliseconds(batchTimeoutMs);
    }

    /// <summary>
    /// 启动消费者
    /// </summary>
    public override void Start()
    {
        base.Start();
        StartBatchTimer();
    }

    /// <summary>
    /// 停止消费者
    /// </summary>
    public override void Stop()
    {
        StopBatchTimer();
        FlushBatch();
        base.Stop();
    }

    private void StartBatchTimer()
    {
        _batchTimer = new Timer(_ => FlushBatch(), null, _batchTimeout, _batchTimeout);
    }

    private void StopBatchTimer()
    {
        _batchTimer?.Change(Timeout.Infinite, Timeout.Infinite);
        _batchTimer?.Dispose();
        _batchTimer = null;
    }

    protected override async void OnConsumerReceived(object? sender, BasicDeliverEventArgs e)
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

            lock (_batchLock)
            {
                _batchMessages.Add(context);
            }

            Logger.LogDebug("消息已加入批量队列, MessageId: {MessageId}, 当前批量大小: {BatchSize}", messageId, _batchMessages.Count);

            if (_batchMessages.Count >= _batchSize)
            {
                await ProcessBatch();
            }
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "处理消息时发生异常, MessageId: {MessageId}", messageId);
            await HandleRetry(e, messageId, retryCount);
        }
    }

    private async Task ProcessBatch()
    {
        List<MessageContext<T>> batch;
        lock (_batchLock)
        {
            if (_batchMessages.Count == 0) return;
            batch = new List<MessageContext<T>>(_batchMessages);
            _batchMessages.Clear();
        }

        try
        {
            Logger.LogInformation("开始处理批量消息, 数量: {Count}", batch.Count);

            if (OnBatchMessageReceived != null)
            {
                var result = await OnBatchMessageReceived(batch);

                if (result == MessageProcessResult.Success)
                {
                    foreach (var msg in batch)
                    {
                        AckMessage(msg.DeliveryTag);
                    }
                    Logger.LogInformation("批量消息处理成功, 数量: {Count}", batch.Count);
                }
                else
                {
                    foreach (var msg in batch)
                    {
                        NackMessage(msg.DeliveryTag, requeue: true);
                    }
                    Logger.LogWarning("批量消息处理失败，重新入队, 数量: {Count}", batch.Count);
                }
            }
            else
            {
                Logger.LogWarning("未设置批量消息处理函数，直接确认所有消息");
                foreach (var msg in batch)
                {
                    AckMessage(msg.DeliveryTag);
                }
            }
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "批量处理消息时发生异常, 数量: {Count}", batch.Count);
            foreach (var msg in batch)
            {
                NackMessage(msg.DeliveryTag, requeue: true);
            }
        }
    }

    private void FlushBatch()
    {
        if (_batchMessages.Count > 0)
        {
            _ = ProcessBatch();
        }
    }
}

/// <summary>
/// 死信队列消费者
/// 用于消费死信队列中的消息，进行异常处理、告警通知等
/// 使用场景：监控失败消息、告警通知、人工干预队列等
/// </summary>
/// <typeparam name="T">消息类型</typeparam>
public class DeadLetterConsumer<T> : ConsumerBase<T> where T : class
{
    /// <summary>
    /// 死信消息处理函数
    /// </summary>
    public Func<MessageContext<T>, Task>? OnDeadLetterReceived { get; set; }

    /// <summary>
    /// 构造函数
    /// </summary>
    /// <param name="connectionManager">连接管理器</param>
    /// <param name="deadLetterQueueName">死信队列名称</param>
    /// <param name="logger">日志记录器</param>
    public DeadLetterConsumer(
        IRabbitMQConnectionManager connectionManager,
        string deadLetterQueueName,
        ILogger<DeadLetterConsumer<T>> logger)
        : base(connectionManager, new ConsumerOptions
        {
            QueueName = deadLetterQueueName,
            AutoAck = true,
            PrefetchCount = 1,
            MaxRetryCount = 0
        }, logger)
    {
    }

    protected override async void OnConsumerReceived(object? sender, BasicDeliverEventArgs e)
    {
        var messageId = e.BasicProperties?.MessageId ?? string.Empty;

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
                RetryCount = GetRetryCount(e.BasicProperties?.Headers),
                DeliveryTag = e.DeliveryTag,
                Headers = e.BasicProperties?.Headers
            };

            Logger.LogWarning("收到死信消息, MessageId: {MessageId}, 队列: {QueueName}", messageId, Options.QueueName);

            if (OnDeadLetterReceived != null)
            {
                await OnDeadLetterReceived(context);
            }
            else
            {
                Logger.LogWarning("未设置死信消息处理函数，仅记录日志, MessageId: {MessageId}", messageId);
            }
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "处理死信消息时发生异常, MessageId: {MessageId}", messageId);
        }
    }
}
