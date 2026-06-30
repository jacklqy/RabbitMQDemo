namespace RabbitMQWrapper.Models;

/// <summary>
/// 消息发布结果
/// </summary>
public class PublishResult
{
    /// <summary>
    /// 是否成功
    /// </summary>
    public bool Success { get; set; }

    /// <summary>
    /// 消息 ID
    /// </summary>
    public string? MessageId { get; set; }

    /// <summary>
    /// 错误信息
    /// </summary>
    public string? ErrorMessage { get; set; }

    /// <summary>
    /// 创建成功结果
    /// </summary>
    public static PublishResult Ok(string messageId)
    {
        return new PublishResult { Success = true, MessageId = messageId };
    }

    /// <summary>
    /// 创建失败结果
    /// </summary>
    public static PublishResult Fail(string errorMessage)
    {
        return new PublishResult { Success = false, ErrorMessage = errorMessage };
    }
}

/// <summary>
/// 消息处理结果
/// </summary>
public enum MessageProcessResult
{
    /// <summary>
    /// 处理成功，确认消息
    /// </summary>
    Success,

    /// <summary>
    /// 处理失败，重试消息
    /// </summary>
    Retry,

    /// <summary>
    /// 处理失败，拒绝消息（进入死信队列或丢弃）
    /// </summary>
    Reject
}

/// <summary>
/// 消息上下文
/// </summary>
/// <typeparam name="T">消息体类型</typeparam>
public class MessageContext<T> where T : class
{
    /// <summary>
    /// 消息体
    /// </summary>
    public T Body { get; set; } = null!;

    /// <summary>
    /// 消息 ID
    /// </summary>
    public string MessageId { get; set; } = string.Empty;

    /// <summary>
    /// 路由键
    /// </summary>
    public string RoutingKey { get; set; } = string.Empty;

    /// <summary>
    /// 交换机名称
    /// </summary>
    public string Exchange { get; set; } = string.Empty;

    /// <summary>
    /// 重试次数
    /// </summary>
    public int RetryCount { get; set; }

    /// <summary>
    /// 消息投递标签
    /// </summary>
    public ulong DeliveryTag { get; set; }

    /// <summary>
    /// 消息头部
    /// </summary>
    public IDictionary<string, object>? Headers { get; set; }
}
