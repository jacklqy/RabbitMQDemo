using Microsoft.AspNetCore.Mvc;
using RabbitMQDemo.Api.Models;
using RabbitMQWrapper.ChannelPool;
using RabbitMQWrapper.Consumers;
using RabbitMQWrapper.Models;
using RabbitMQWrapper.Options;
using RabbitMQWrapper.Connection;

namespace RabbitMQDemo.Api.Controllers;

/// <summary>
/// 消费者演示控制器
/// 展示如何手动创建和管理消费者
/// </summary>
[ApiController]
[Route("api/[controller]")]
public class ConsumerDemoController : ControllerBase
{
    private readonly IRabbitMQConnectionManager _connectionManager;
    private readonly IChannelPool _channelPool;
    private readonly ILoggerFactory _loggerFactory;
    private readonly ILogger<ConsumerDemoController> _logger;
    private static BasicConsumer<OrderMessage>? _manualConsumer;
    private static DeadLetterConsumer<OrderMessage>? _deadLetterConsumer;

    public ConsumerDemoController(
        IRabbitMQConnectionManager connectionManager,
        IChannelPool channelPool,
        ILoggerFactory loggerFactory,
        ILogger<ConsumerDemoController> logger)
    {
        _connectionManager = connectionManager;
        _channelPool = channelPool;
        _loggerFactory = loggerFactory;
        _logger = logger;
    }

    /// <summary>
    /// 启动手动消费者
    /// 展示如何手动创建并启动消费者
    /// </summary>
    /// <returns>操作结果</returns>
    [HttpPost("manual/start")]
    public IActionResult StartManualConsumer()
    {
        if (_manualConsumer != null)
        {
            return BadRequest("消费者已在运行中");
        }

        var options = new ConsumerOptions
        {
            QueueName = "demo.manual.queue",
            PrefetchCount = 1,
            MaxRetryCount = 3,
            RetryInterval = 1000,
            AutoAck = false
        };

        _manualConsumer = new BasicConsumer<OrderMessage>(
            _connectionManager,
            options,
            _loggerFactory.CreateLogger<BasicConsumer<OrderMessage>>());

        _manualConsumer.OnMessageReceived = HandleManualMessageAsync;
        _manualConsumer.Start();

        _logger.LogInformation("手动消费者已启动");
        return Ok("手动消费者已启动");
    }

    /// <summary>
    /// 停止手动消费者
    /// </summary>
    /// <returns>操作结果</returns>
    [HttpPost("manual/stop")]
    public IActionResult StopManualConsumer()
    {
        if (_manualConsumer == null)
        {
            return BadRequest("消费者未运行");
        }

        _manualConsumer.Stop();
        _manualConsumer = null;

        _logger.LogInformation("手动消费者已停止");
        return Ok("手动消费者已停止");
    }

    /// <summary>
    /// 发送消息到手动消费者队列
    /// </summary>
    /// <param name="message">消息内容</param>
    /// <returns>操作结果</returns>
    [HttpPost("manual/send")]
    public IActionResult SendToManualQueue([FromBody] OrderMessage message)
    {
        _channelPool.UseChannel(channel =>
        {
            channel.QueueDeclare("demo.manual.queue", durable: true, exclusive: false, autoDelete: false, arguments: null);

            var body = System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(message);
            var properties = channel.CreateBasicProperties();
            properties.MessageId = Guid.NewGuid().ToString();
            properties.DeliveryMode = 2;

            channel.BasicPublish(string.Empty, "demo.manual.queue", properties, body);
        });

        return Ok("消息已发送");
    }

    /// <summary>
    /// 启动死信队列消费者
    /// 用于监控死信队列中的消息
    /// </summary>
    /// <returns>操作结果</returns>
    [HttpPost("deadletter/start")]
    public IActionResult StartDeadLetterConsumer()
    {
        if (_deadLetterConsumer != null)
        {
            return BadRequest("死信队列消费者已在运行中");
        }

        _deadLetterConsumer = new DeadLetterConsumer<OrderMessage>(
            _connectionManager,
            "demo.order.deadletter.queue",
            _loggerFactory.CreateLogger<DeadLetterConsumer<OrderMessage>>());

        _deadLetterConsumer.OnDeadLetterReceived = HandleDeadLetterAsync;
        _deadLetterConsumer.Start();

        _logger.LogInformation("死信队列消费者已启动");
        return Ok("死信队列消费者已启动");
    }

    /// <summary>
    /// 停止死信队列消费者
    /// </summary>
    /// <returns>操作结果</returns>
    [HttpPost("deadletter/stop")]
    public IActionResult StopDeadLetterConsumer()
    {
        if (_deadLetterConsumer == null)
        {
            return BadRequest("死信队列消费者未运行");
        }

        _deadLetterConsumer.Stop();
        _deadLetterConsumer = null;

        _logger.LogInformation("死信队列消费者已停止");
        return Ok("死信队列消费者已停止");
    }

    private Task<MessageProcessResult> HandleManualMessageAsync(MessageContext<OrderMessage> context)
    {
        _logger.LogInformation("手动消费者处理消息: OrderId={OrderId}, 重试次数={RetryCount}",
            context.Body.OrderId, context.RetryCount);

        // 模拟业务处理
        // 第1、2次失败，第3次成功（演示重试机制）
        if (context.RetryCount < 2)
        {
            _logger.LogWarning("模拟处理失败，准备重试");
            return Task.FromResult(MessageProcessResult.Retry);
        }

        _logger.LogInformation("消息处理成功");
        return Task.FromResult(MessageProcessResult.Success);
    }

    private Task HandleDeadLetterAsync(MessageContext<OrderMessage> context)
    {
        _logger.LogError("收到死信消息，需要人工处理: OrderId={OrderId}", context.Body.OrderId);

        // TODO: 死信消息处理逻辑
        // 例如：发送告警、写入数据库、人工处理队列等

        return Task.CompletedTask;
    }
}
