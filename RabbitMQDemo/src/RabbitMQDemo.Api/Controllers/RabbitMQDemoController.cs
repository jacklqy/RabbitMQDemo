using Microsoft.AspNetCore.Mvc;
using RabbitMQDemo.Api.Models;
using RabbitMQWrapper.Models;
using RabbitMQWrapper.Options;
using RabbitMQWrapper.Producer;

namespace RabbitMQDemo.Api.Controllers;

/// <summary>
/// RabbitMQ 演示控制器
/// 展示各种 RabbitMQ 使用场景
/// </summary>
[ApiController]
[Route("api/[controller]")]
public class RabbitMQDemoController : ControllerBase
{
    private readonly IRabbitMQProducer _producer;
    private readonly ILogger<RabbitMQDemoController> _logger;

    public RabbitMQDemoController(IRabbitMQProducer producer, ILogger<RabbitMQDemoController> logger)
    {
        _producer = producer;
        _logger = logger;
    }

    #region 基础消息发布

    /// <summary>
    /// 发布简单消息（队列模式）
    /// 直接发送消息到指定队列
    /// 使用场景：最简单的点对点消息传递
    /// </summary>
    /// <param name="message">消息内容</param>
    /// <returns>发布结果</returns>
    [HttpPost("basic/simple")]
    public IActionResult PublishSimpleMessage([FromBody] OrderMessage message)
    {
        _logger.LogInformation("发布简单消息，订单号: {OrderId}", message.OrderId);
        var result = _producer.Publish("demo.order.queue", message);
        return Ok(result);
    }

    /// <summary>
    /// 批量发布消息
    /// 一次性发送多条消息到队列
    /// 使用场景：批量数据导入、批量任务创建
    /// </summary>
    /// <param name="count">消息数量</param>
    /// <returns>发布结果</returns>
    [HttpPost("basic/batch/{count}")]
    public IActionResult BatchPublishMessages(int count)
    {
        _logger.LogInformation("批量发布 {Count} 条消息", count);

        var messages = new List<OrderMessage>();
        for (int i = 0; i < count; i++)
        {
            messages.Add(new OrderMessage
            {
                OrderId = $"ORDER-{DateTime.Now:yyyyMMdd}-{i + 1}",
                OrderName = $"测试订单{i + 1}",
                Amount = new Random().Next(100, 10000)
            });
        }

        var results = _producer.BatchPublish("demo.order.queue", messages);
        return Ok(new { Total = results.Count, SuccessCount = results.Count(r => r.Success), Results = results });
    }

    #endregion

    #region 交换机模式

    /// <summary>
    /// 发布消息到交换机（Direct 模式）
    /// 使用场景：根据路由键精确匹配投递消息
    /// </summary>
    /// <param name="message">用户注册消息</param>
    /// <returns>发布结果</returns>
    [HttpPost("exchange/direct")]
    public IActionResult PublishToDirectExchange([FromBody] UserRegisterMessage message)
    {
        _logger.LogInformation("发布用户注册消息到 Direct 交换机，用户: {UserId}", message.UserId);

        const string exchangeName = "demo.user.register.exchange";

        var emailResult = _producer.PublishToExchange(exchangeName, "email", message, "direct");
        var smsResult = _producer.PublishToExchange(exchangeName, "sms", message, "direct");

        return Ok(new
        {
            EmailResult = emailResult,
            SmsResult = smsResult
        });
    }

    /// <summary>
    /// 发布消息到交换机（Fanout 模式）
    /// 使用场景：广播消息，所有绑定的队列都能收到
    /// </summary>
    /// <param name="content">广播内容</param>
    /// <returns>发布结果</returns>
    [HttpPost("exchange/fanout")]
    public IActionResult PublishToFanoutExchange([FromBody] string content)
    {
        _logger.LogInformation("发布广播消息到 Fanout 交换机");

        const string exchangeName = "demo.fanout.exchange";
        var message = new { Content = content, Time = DateTime.Now };

        var result = _producer.PublishToExchange(exchangeName, string.Empty, message, "fanout");
        return Ok(result);
    }

    /// <summary>
    /// 发布消息到交换机（Topic 模式）
    /// 使用场景：根据通配符路由，支持模糊匹配
    /// </summary>
    /// <param name="routingKey">路由键</param>
    /// <param name="message">消息内容</param>
    /// <returns>发布结果</returns>
    [HttpPost("exchange/topic")]
    public IActionResult PublishToTopicExchange([FromQuery] string routingKey, [FromBody] string message)
    {
        _logger.LogInformation("发布消息到 Topic 交换机，路由键: {RoutingKey}", routingKey);

        const string exchangeName = "demo.topic.exchange";
        var msg = new { RoutingKey = routingKey, Content = message, Time = DateTime.Now };

        var result = _producer.PublishToExchange(exchangeName, routingKey, msg, "topic");
        return Ok(result);
    }

    #endregion

    #region 死信队列

    /// <summary>
    /// 发布消息（带死信队列）
    /// 使用场景：消息处理失败时不会丢失，进入死信队列供后续处理
    /// </summary>
    /// <param name="message">消息内容</param>
    /// <returns>发布结果</returns>
    [HttpPost("deadletter")]
    public IActionResult PublishWithDeadLetter([FromBody] OrderMessage message)
    {
        _logger.LogInformation("发布带死信队列的消息，订单号: {OrderId}", message.OrderId);

        var deadLetterOptions = new DeadLetterOptions
        {
            Enabled = true,
            DeadLetterExchange = "demo.order.dlx",
            DeadLetterRoutingKey = "demo.order.deadletter",
            DeadLetterQueueName = "demo.order.deadletter.queue",
            MaxLength = 10000
        };

        var result = _producer.PublishWithDeadLetter("demo.order.dlx.queue", message, deadLetterOptions);
        return Ok(result);
    }

    /// <summary>
    /// 发布消息（带 TTL 过期时间）
    /// 使用场景：消息在指定时间内未被消费则进入死信队列
    /// </summary>
    /// <param name="message">消息内容</param>
    /// <param name="ttlSeconds">过期时间（秒）</param>
    /// <returns>发布结果</returns>
    [HttpPost("deadletter/ttl")]
    public IActionResult PublishWithTtl([FromBody] OrderMessage message, [FromQuery] int ttlSeconds = 30)
    {
        _logger.LogInformation("发布带 TTL 的消息，订单号: {OrderId}, TTL: {Ttl}秒", message.OrderId, ttlSeconds);

        var deadLetterOptions = new DeadLetterOptions
        {
            Enabled = true,
            DeadLetterExchange = "demo.ttl.dlx",
            DeadLetterRoutingKey = "demo.ttl.deadletter",
            DeadLetterQueueName = "demo.ttl.deadletter.queue",
            MessageTtl = ttlSeconds * 1000
        };

        var result = _producer.PublishWithDeadLetter("demo.ttl.queue", message, deadLetterOptions);
        return Ok(result);
    }

    #endregion

    #region 延迟消息

    /// <summary>
    /// 发布延迟消息
    /// 通过死信队列 + TTL 实现延迟消息
    /// 使用场景：订单超时取消、延迟通知、定时任务等
    /// </summary>
    /// <param name="message">支付消息</param>
    /// <param name="delaySeconds">延迟时间（秒）</param>
    /// <returns>发布结果</returns>
    [HttpPost("delayed")]
    public IActionResult PublishDelayedMessage([FromBody] PaymentMessage message, [FromQuery] int delaySeconds = 30)
    {
        _logger.LogInformation("发布延迟消息，支付单号: {PaymentId}, 延迟: {Delay}秒", message.PaymentId, delaySeconds);

        var result = _producer.PublishDelayed("demo.payment.timeout.queue", message, delaySeconds * 1000);
        return Ok(result);
    }

    /// <summary>
    /// 发布订单超时取消延迟消息
    /// 使用场景：订单创建后30分钟未支付则自动取消
    /// </summary>
    /// <param name="orderId">订单号</param>
    /// <param name="delayMinutes">延迟时间（分钟）</param>
    /// <returns>发布结果</returns>
    [HttpPost("delayed/order-timeout")]
    public IActionResult PublishOrderTimeoutMessage([FromQuery] string orderId, [FromQuery] int delayMinutes = 30)
    {
        _logger.LogInformation("发布订单超时消息，订单号: {OrderId}, 延迟: {Delay}分钟", orderId, delayMinutes);

        var message = new
        {
            OrderId = orderId,
            Reason = "订单超时未支付自动取消",
            CreateTime = DateTime.Now
        };

        var result = _producer.PublishDelayed("demo.order.timeout.queue", message, delayMinutes * 60 * 1000);
        return Ok(result);
    }

    #endregion

    #region 高并发演示

    /// <summary>
    /// 高并发发布消息测试
    /// 使用通道池支持高并发场景
    /// </summary>
    /// <param name="count">消息数量</param>
    /// <param name="parallelCount">并发数</param>
    /// <returns>测试结果</returns>
    [HttpPost("concurrent")]
    public async Task<IActionResult> ConcurrentPublishTest(int count = 1000, int parallelCount = 10)
    {
        _logger.LogInformation("高并发发布测试，总数: {Count}, 并发数: {ParallelCount}", count, parallelCount);

        var startTime = DateTime.Now;
        var successCount = 0;
        var failCount = 0;
        var lockObj = new object();

        var parallelOptions = new ParallelOptions
        {
            MaxDegreeOfParallelism = parallelCount
        };

        await Task.Run(() =>
        {
            Parallel.For(0, count, parallelOptions, i =>
            {
                try
                {
                    var message = new OrderMessage
                    {
                        OrderId = $"CONCURRENT-{i + 1}",
                        OrderName = $"并发测试订单{i + 1}",
                        Amount = new Random().Next(100, 10000)
                    };

                    var result = _producer.Publish("demo.concurrent.queue", message);

                    lock (lockObj)
                    {
                        if (result.Success)
                            successCount++;
                        else
                            failCount++;
                    }
                }
                catch
                {
                    lock (lockObj)
                    {
                        failCount++;
                    }
                }
            });
        });

        var elapsed = DateTime.Now - startTime;

        return Ok(new
        {
            Total = count,
            SuccessCount = successCount,
            FailCount = failCount,
            ElapsedMilliseconds = elapsed.TotalMilliseconds,
            Tps = Math.Round(count / elapsed.TotalSeconds, 2)
        });
    }

    #endregion
}
