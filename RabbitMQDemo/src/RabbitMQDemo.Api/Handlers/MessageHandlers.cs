using Microsoft.Extensions.Logging;
using RabbitMQDemo.Api.Models;
using RabbitMQWrapper.DependencyInjection;
using RabbitMQWrapper.Models;

namespace RabbitMQDemo.Api.Handlers;

/// <summary>
/// 订单消息处理器
/// 实现 IMessageHandler 接口，处理订单消息
/// 使用场景：订单创建后处理库存、通知用户等
/// </summary>
public class OrderMessageHandler : IMessageHandler<OrderMessage>
{
    private readonly ILogger<OrderMessageHandler> _logger;

    public OrderMessageHandler(ILogger<OrderMessageHandler> logger)
    {
        _logger = logger;
    }

    /// <summary>
    /// 处理订单消息
    /// </summary>
    /// <param name="context">消息上下文</param>
    /// <returns>处理结果</returns>
    public Task<MessageProcessResult> HandleAsync(MessageContext<OrderMessage> context)
    {
        try
        {
            var order = context.Body;
            _logger.LogInformation("处理订单消息: OrderId={OrderId}, OrderName={OrderName}, Amount={Amount}, 重试次数={RetryCount}",
                order.OrderId, order.OrderName, order.Amount, context.RetryCount);

            // TODO: 在这里添加业务逻辑
            // 例如：扣减库存、发送通知、更新订单状态等

            return Task.FromResult(MessageProcessResult.Success);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "处理订单消息失败: OrderId={OrderId}", context.Body.OrderId);
            return Task.FromResult(MessageProcessResult.Retry);
        }
    }
}

/// <summary>
/// 用户注册消息处理器（发送邮件）
/// 使用场景：用户注册后发送欢迎邮件
/// </summary>
public class UserRegisterEmailHandler : IMessageHandler<UserRegisterMessage>
{
    private readonly ILogger<UserRegisterEmailHandler> _logger;

    public UserRegisterEmailHandler(ILogger<UserRegisterEmailHandler> logger)
    {
        _logger = logger;
    }

    public Task<MessageProcessResult> HandleAsync(MessageContext<UserRegisterMessage> context)
    {
        var user = context.Body;
        _logger.LogInformation("发送注册邮件: UserId={UserId}, Email={Email}", user.UserId, user.Email);

        // TODO: 发送邮件逻辑

        return Task.FromResult(MessageProcessResult.Success);
    }
}

/// <summary>
/// 用户注册消息处理器（发送短信）
/// 使用场景：用户注册后发送短信通知
/// </summary>
public class UserRegisterSmsHandler : IMessageHandler<UserRegisterMessage>
{
    private readonly ILogger<UserRegisterSmsHandler> _logger;

    public UserRegisterSmsHandler(ILogger<UserRegisterSmsHandler> logger)
    {
        _logger = logger;
    }

    public Task<MessageProcessResult> HandleAsync(MessageContext<UserRegisterMessage> context)
    {
        var user = context.Body;
        _logger.LogInformation("发送注册短信: UserId={UserId}, Phone={Phone}", user.UserId, user.Phone);

        // TODO: 发送短信逻辑

        return Task.FromResult(MessageProcessResult.Success);
    }
}

/// <summary>
/// 日志批量处理器
/// 使用场景：批量写入日志到数据库
/// </summary>
public class LogBatchHandler
{
    private readonly ILogger<LogBatchHandler> _logger;

    public LogBatchHandler(ILogger<LogBatchHandler> logger)
    {
        _logger = logger;
    }

    /// <summary>
    /// 批量处理日志
    /// </summary>
    /// <param name="messages">日志消息列表</param>
    /// <returns>处理结果</returns>
    public Task<MessageProcessResult> HandleBatchAsync(List<MessageContext<LogMessage>> messages)
    {
        _logger.LogInformation("批量处理日志消息，数量: {Count}", messages.Count);

        // TODO: 批量写入数据库

        return Task.FromResult(MessageProcessResult.Success);
    }
}
