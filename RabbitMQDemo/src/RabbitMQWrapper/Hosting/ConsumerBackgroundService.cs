using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RabbitMQWrapper.Consumers;
using RabbitMQWrapper.Models;
using RabbitMQWrapper.Options;

namespace RabbitMQWrapper.Hosting;

/// <summary>
/// 消费者后台服务
/// 继承自 BackgroundService，在应用启动时自动启动消费者
/// 应用关闭时自动停止消费者
/// 使用场景：需要持续消费消息的后台服务
/// </summary>
/// <typeparam name="T">消息类型</typeparam>
public class ConsumerBackgroundService<T> : BackgroundService where T : class
{
    private readonly IServiceProvider _serviceProvider;
    private readonly ConsumerOptions _options;
    private readonly ILogger<ConsumerBackgroundService<T>> _logger;
    private IConsumer<T>? _consumer;

    /// <summary>
    /// 构造函数
    /// </summary>
    /// <param name="serviceProvider">服务提供者</param>
    /// <param name="options">消费者配置</param>
    /// <param name="logger">日志记录器</param>
    public ConsumerBackgroundService(
        IServiceProvider serviceProvider,
        ConsumerOptions options,
        ILogger<ConsumerBackgroundService<T>> logger)
    {
        _serviceProvider = serviceProvider;
        _options = options;
        _logger = logger;
    }

    /// <summary>
    /// 执行后台服务
    /// </summary>
    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("正在启动消费者后台服务，队列: {QueueName}", _options.QueueName);

        try
        {
            var scope = _serviceProvider.CreateScope();
            var consumer = scope.ServiceProvider.GetRequiredService<IConsumer<T>>();

            consumer.OnMessageReceived = HandleMessageAsync;
            consumer.Start();
            _consumer = consumer;

            _logger.LogInformation("消费者后台服务已启动，队列: {QueueName}", _options.QueueName);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "启动消费者后台服务失败，队列: {QueueName}", _options.QueueName);
        }

        return Task.CompletedTask;
    }

    /// <summary>
    /// 停止后台服务
    /// </summary>
    public override Task StopAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("正在停止消费者后台服务，队列: {QueueName}", _options.QueueName);

        try
        {
            _consumer?.Stop();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "停止消费者后台服务时发生异常，队列: {QueueName}", _options.QueueName);
        }

        return base.StopAsync(cancellationToken);
    }

    /// <summary>
    /// 处理消息
    /// 可在子类中重写此方法
    /// </summary>
    protected virtual Task<MessageProcessResult> HandleMessageAsync(MessageContext<T> context)
    {
        _logger.LogInformation("收到消息, MessageId: {MessageId}", context.MessageId);
        return Task.FromResult(MessageProcessResult.Success);
    }
}

/// <summary>
/// 批量消费者后台服务
/// </summary>
/// <typeparam name="T">消息类型</typeparam>
public class BatchConsumerBackgroundService<T> : BackgroundService where T : class
{
    private readonly IServiceProvider _serviceProvider;
    private readonly ConsumerOptions _options;
    private readonly int _batchSize;
    private readonly int _batchTimeoutMs;
    private readonly ILogger<BatchConsumerBackgroundService<T>> _logger;
    private BatchConsumer<T>? _consumer;

    /// <summary>
    /// 构造函数
    /// </summary>
    public BatchConsumerBackgroundService(
        IServiceProvider serviceProvider,
        ConsumerOptions options,
        ILogger<BatchConsumerBackgroundService<T>> logger,
        int batchSize = 100,
        int batchTimeoutMs = 5000)
    {
        _serviceProvider = serviceProvider;
        _options = options;
        _batchSize = batchSize;
        _batchTimeoutMs = batchTimeoutMs;
        _logger = logger;
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("正在启动批量消费者后台服务，队列: {QueueName}", _options.QueueName);

        try
        {
            var scope = _serviceProvider.CreateScope();
            var consumer = scope.ServiceProvider.GetRequiredService<BatchConsumer<T>>();

            consumer.OnBatchMessageReceived = HandleBatchMessageAsync;
            consumer.Start();
            _consumer = consumer;

            _logger.LogInformation("批量消费者后台服务已启动，队列: {QueueName}", _options.QueueName);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "启动批量消费者后台服务失败，队列: {QueueName}", _options.QueueName);
        }

        return Task.CompletedTask;
    }

    public override Task StopAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("正在停止批量消费者后台服务，队列: {QueueName}", _options.QueueName);

        try
        {
            _consumer?.Stop();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "停止批量消费者后台服务时发生异常，队列: {QueueName}", _options.QueueName);
        }

        return base.StopAsync(cancellationToken);
    }

    protected virtual Task<MessageProcessResult> HandleBatchMessageAsync(List<MessageContext<T>> messages)
    {
        _logger.LogInformation("收到批量消息, 数量: {Count}", messages.Count);
        return Task.FromResult(MessageProcessResult.Success);
    }
}
