namespace RabbitMQWrapper.Options;

/// <summary>
/// 队列声明配置
/// </summary>
public class QueueDeclareOptions
{
    /// <summary>
    /// 队列名称
    /// </summary>
    public string QueueName { get; set; } = string.Empty;

    /// <summary>
    /// 是否持久化，默认 true
    /// </summary>
    public bool Durable { get; set; } = true;

    /// <summary>
    /// 是否排他队列，默认 false
    /// </summary>
    public bool Exclusive { get; set; } = false;

    /// <summary>
    /// 是否自动删除，默认 false
    /// </summary>
    public bool AutoDelete { get; set; } = false;

    /// <summary>
    /// 队列额外参数
    /// </summary>
    public Dictionary<string, object>? Arguments { get; set; }
}

/// <summary>
/// 交换机声明配置
/// </summary>
public class ExchangeDeclareOptions
{
    /// <summary>
    /// 交换机名称
    /// </summary>
    public string ExchangeName { get; set; } = string.Empty;

    /// <summary>
    /// 交换机类型：direct、topic、fanout、headers
    /// </summary>
    public string Type { get; set; } = "direct";

    /// <summary>
    /// 是否持久化，默认 true
    /// </summary>
    public bool Durable { get; set; } = true;

    /// <summary>
    /// 是否自动删除，默认 false
    /// </summary>
    public bool AutoDelete { get; set; } = false;

    /// <summary>
    /// 交换机额外参数
    /// </summary>
    public Dictionary<string, object>? Arguments { get; set; }
}

/// <summary>
/// 死信队列配置
/// </summary>
public class DeadLetterOptions
{
    /// <summary>
    /// 是否启用死信队列，默认 false
    /// </summary>
    public bool Enabled { get; set; } = false;

    /// <summary>
    /// 死信交换机名称
    /// </summary>
    public string? DeadLetterExchange { get; set; }

    /// <summary>
    /// 死信路由键
    /// </summary>
    public string? DeadLetterRoutingKey { get; set; }

    /// <summary>
    /// 死信队列名称（不设置则自动生成）
    /// </summary>
    public string? DeadLetterQueueName { get; set; }

    /// <summary>
    /// 消息 TTL（毫秒），设置后消息过期会进入死信队列
    /// </summary>
    public int? MessageTtl { get; set; }

    /// <summary>
    /// 队列最大长度，超出后新消息进入死信队列
    /// </summary>
    public int? MaxLength { get; set; }
}

/// <summary>
/// 消费者配置
/// </summary>
public class ConsumerOptions
{
    /// <summary>
    /// 队列名称
    /// </summary>
    public string QueueName { get; set; } = string.Empty;

    /// <summary>
    /// 消费者数量，默认 1
    /// </summary>
    public int ConsumerCount { get; set; } = 1;

    /// <summary>
    /// 是否自动确认，默认 false（手动确认更可靠）
    /// </summary>
    public bool AutoAck { get; set; } = false;

    /// <summary>
    /// 预取消息数量（QoS），默认 1
    /// </summary>
    public ushort PrefetchCount { get; set; } = 1;

    /// <summary>
    /// 最大重试次数，默认 3 次
    /// </summary>
    public int MaxRetryCount { get; set; } = 3;

    /// <summary>
    /// 重试间隔（毫秒），默认 1000ms
    /// </summary>
    public int RetryInterval { get; set; } = 1000;

    /// <summary>
    /// 消费者标签
    /// </summary>
    public string? ConsumerTag { get; set; }

    /// <summary>
    /// 是否启用死信队列（消费失败达到最大重试次数后进入死信队列）
    /// </summary>
    public bool EnableDeadLetter { get; set; } = true;
}
