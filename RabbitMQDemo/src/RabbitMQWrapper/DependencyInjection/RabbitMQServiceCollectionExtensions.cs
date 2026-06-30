using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using RabbitMQWrapper.ChannelPool;
using RabbitMQWrapper.Connection;
using RabbitMQWrapper.Consumers;
using RabbitMQWrapper.Hosting;
using RabbitMQWrapper.Options;
using RabbitMQWrapper.Producer;

namespace RabbitMQWrapper.DependencyInjection;

/// <summary>
/// RabbitMQ 依赖注入扩展方法
/// 提供便捷的注册方式，支持多种使用场景
/// </summary>
public static class RabbitMQServiceCollectionExtensions
{
    /// <summary>
    /// 添加 RabbitMQ 核心服务
    /// 注册连接管理器、通道池、生产者等核心组件
    /// 连接管理器采用单例模式，确保整个应用只有一个连接
    /// </summary>
    /// <param name="services">服务集合</param>
    /// <param name="configuration">配置对象</param>
    /// <param name="sectionName">配置节名称，默认 RabbitMQ</param>
    /// <returns>服务集合</returns>
    public static IServiceCollection AddRabbitMQ(
        this IServiceCollection services,
        IConfiguration configuration,
        string sectionName = RabbitMQOptions.SectionName)
    {
        services.Configure<RabbitMQOptions>(configuration.GetSection(sectionName));

        services.AddSingleton<IRabbitMQConnectionManager, RabbitMQConnectionManager>();
        services.AddSingleton<IChannelPool, ChannelPool.ChannelPool>();
        services.AddSingleton<IRabbitMQProducer, RabbitMQProducer>();

        return services;
    }

    /// <summary>
    /// 添加 RabbitMQ 核心服务（使用配置委托）
    /// </summary>
    /// <param name="services">服务集合</param>
    /// <param name="configureOptions">配置委托</param>
    /// <returns>服务集合</returns>
    public static IServiceCollection AddRabbitMQ(
        this IServiceCollection services,
        Action<RabbitMQOptions> configureOptions)
    {
        services.Configure(configureOptions);

        services.AddSingleton<IRabbitMQConnectionManager, RabbitMQConnectionManager>();
        services.AddSingleton<IChannelPool, ChannelPool.ChannelPool>();
        services.AddSingleton<IRabbitMQProducer, RabbitMQProducer>();

        return services;
    }

    /// <summary>
    /// 添加基础消费者（泛型）
    /// 注册指定类型的基础消费者
    /// </summary>
    /// <typeparam name="T">消息类型</typeparam>
    /// <param name="services">服务集合</param>
    /// <param name="configure">消费者配置委托</param>
    /// <returns>服务集合</returns>
    public static IServiceCollection AddBasicConsumer<T>(
        this IServiceCollection services,
        Action<ConsumerOptions> configure) where T : class
    {
        var options = new ConsumerOptions();
        configure(options);

        services.AddSingleton(options);
        services.AddScoped<IConsumer<T>, BasicConsumer<T>>();

        return services;
    }

    /// <summary>
    /// 添加基础消费者并注册为后台服务
    /// 应用启动时自动开始消费
    /// </summary>
    /// <typeparam name="T">消息类型</typeparam>
    /// <typeparam name="THandler">消息处理器类型</typeparam>
    /// <param name="services">服务集合</param>
    /// <param name="configure">消费者配置委托</param>
    /// <returns>服务集合</returns>
    public static IServiceCollection AddBasicConsumerHostedService<T, THandler>(
        this IServiceCollection services,
        Action<ConsumerOptions> configure)
        where T : class
        where THandler : class, IMessageHandler<T>
    {
        var options = new ConsumerOptions();
        configure(options);

        services.AddSingleton(options);
        services.AddScoped<IConsumer<T>, BasicConsumer<T>>();
        services.AddScoped<IMessageHandler<T>, THandler>();

        services.AddHostedService<MessageHandlerBackgroundService<T, THandler>>();

        return services;
    }

    /// <summary>
    /// 添加批量消费者
    /// </summary>
    /// <typeparam name="T">消息类型</typeparam>
    /// <param name="services">服务集合</param>
    /// <param name="configure">消费者配置委托</param>
    /// <param name="batchSize">批量大小，默认 100</param>
    /// <param name="batchTimeoutMs">批量超时时间（毫秒），默认 5000ms</param>
    /// <returns>服务集合</returns>
    public static IServiceCollection AddBatchConsumer<T>(
        this IServiceCollection services,
        Action<ConsumerOptions> configure,
        int batchSize = 100,
        int batchTimeoutMs = 5000) where T : class
    {
        var options = new ConsumerOptions();
        configure(options);

        services.AddSingleton(options);
        services.AddScoped(sp =>
        {
            var connectionManager = sp.GetRequiredService<IRabbitMQConnectionManager>();
            var logger = sp.GetRequiredService<ILogger<BatchConsumer<T>>>();
            return new BatchConsumer<T>(connectionManager, options, logger, batchSize, batchTimeoutMs);
        });

        return services;
    }

    /// <summary>
    /// 添加死信队列消费者
    /// </summary>
    /// <typeparam name="T">消息类型</typeparam>
    /// <param name="services">服务集合</param>
    /// <param name="deadLetterQueueName">死信队列名称</param>
    /// <returns>服务集合</returns>
    public static IServiceCollection AddDeadLetterConsumer<T>(
        this IServiceCollection services,
        string deadLetterQueueName) where T : class
    {
        services.AddScoped(sp =>
        {
            var connectionManager = sp.GetRequiredService<IRabbitMQConnectionManager>();
            var logger = sp.GetRequiredService<ILogger<DeadLetterConsumer<T>>>();
            return new DeadLetterConsumer<T>(connectionManager, deadLetterQueueName, logger);
        });

        return services;
    }
}

/// <summary>
/// 消息处理器接口
/// 实现此接口来处理具体的消息业务逻辑
/// </summary>
/// <typeparam name="T">消息类型</typeparam>
public interface IMessageHandler<T> where T : class
{
    /// <summary>
    /// 处理消息
    /// </summary>
    /// <param name="context">消息上下文</param>
    /// <returns>处理结果</returns>
    Task<MessageProcessResult> HandleAsync(MessageContext<T> context);
}

/// <summary>
/// 消息处理器后台服务
/// 自动启动消费者并调用消息处理器处理消息
/// </summary>
/// <typeparam name="T">消息类型</typeparam>
/// <typeparam name="THandler">消息处理器类型</typeparam>
public class MessageHandlerBackgroundService<T, THandler> : BackgroundService
    where T : class
    where THandler : class, IMessageHandler<T>
{
    private readonly IServiceProvider _serviceProvider;
    private readonly ConsumerOptions _options;
    private readonly ILogger<MessageHandlerBackgroundService<T, THandler>> _logger;
    private IConsumer<T>? _consumer;

    /// <summary>
    /// 构造函数
    /// </summary>
    public MessageHandlerBackgroundService(
        IServiceProvider serviceProvider,
        ConsumerOptions options,
        ILogger<MessageHandlerBackgroundService<T, THandler>> logger)
    {
        _serviceProvider = serviceProvider;
        _options = options;
        _logger = logger;
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("正在启动消息处理器后台服务，队列: {QueueName}", _options.QueueName);

        try
        {
            var scope = _serviceProvider.CreateScope();
            var consumer = scope.ServiceProvider.GetRequiredService<IConsumer<T>>();
            var handler = scope.ServiceProvider.GetRequiredService<IMessageHandler<T>>();

            consumer.OnMessageReceived = handler.HandleAsync;
            consumer.Start();
            _consumer = consumer;

            _logger.LogInformation("消息处理器后台服务已启动，队列: {QueueName}", _options.QueueName);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "启动消息处理器后台服务失败，队列: {QueueName}", _options.QueueName);
        }

        return Task.CompletedTask;
    }

    public override Task StopAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("正在停止消息处理器后台服务，队列: {QueueName}", _options.QueueName);

        try
        {
            _consumer?.Stop();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "停止消息处理器后台服务时发生异常，队列: {QueueName}", _options.QueueName);
        }

        return base.StopAsync(cancellationToken);
    }
}
