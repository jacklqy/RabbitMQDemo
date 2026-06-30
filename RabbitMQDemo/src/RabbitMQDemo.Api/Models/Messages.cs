namespace RabbitMQDemo.Api.Models;

/// <summary>
/// 订单消息模型
/// 用于演示基础消息发布/消费场景
/// </summary>
public class OrderMessage
{
    /// <summary>
    /// 订单 ID
    /// </summary>
    public string OrderId { get; set; } = string.Empty;

    /// <summary>
    /// 订单名称
    /// </summary>
    public string OrderName { get; set; } = string.Empty;

    /// <summary>
    /// 订单金额
    /// </summary>
    public decimal Amount { get; set; }

    /// <summary>
    /// 创建时间
    /// </summary>
    public DateTime CreateTime { get; set; } = DateTime.Now;
}

/// <summary>
/// 用户注册消息
/// 用于演示发布订阅（交换机）模式
/// </summary>
public class UserRegisterMessage
{
    /// <summary>
    /// 用户 ID
    /// </summary>
    public string UserId { get; set; } = string.Empty;

    /// <summary>
    /// 用户名
    /// </summary>
    public string UserName { get; set; } = string.Empty;

    /// <summary>
    /// 邮箱
    /// </summary>
    public string Email { get; set; } = string.Empty;

    /// <summary>
    /// 手机号
    /// </summary>
    public string Phone { get; set; } = string.Empty;
}

/// <summary>
/// 日志消息
/// 用于演示批量消费场景
/// </summary>
public class LogMessage
{
    /// <summary>
    /// 日志级别
    /// </summary>
    public string Level { get; set; } = "Info";

    /// <summary>
    /// 日志内容
    /// </summary>
    public string Content { get; set; } = string.Empty;

    /// <summary>
    /// 来源
    /// </summary>
    public string Source { get; set; } = string.Empty;

    /// <summary>
    /// 记录时间
    /// </summary>
    public DateTime LogTime { get; set; } = DateTime.Now;
}

/// <summary>
/// 支付消息
/// 用于演示延迟消息场景
/// </summary>
public class PaymentMessage
{
    /// <summary>
    /// 支付 ID
    /// </summary>
    public string PaymentId { get; set; } = string.Empty;

    /// <summary>
    /// 订单 ID
    /// </summary>
    public string OrderId { get; set; } = string.Empty;

    /// <summary>
    /// 支付金额
    /// </summary>
    public decimal Amount { get; set; }

    /// <summary>
    /// 支付状态
    /// </summary>
    public string Status { get; set; } = "Pending";
}
