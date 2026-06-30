namespace RabbitMQWrapper.Options;

/// <summary>
/// RabbitMQ 连接配置选项
/// </summary>
public class RabbitMQOptions
{
    /// <summary>
    /// 配置节名称
    /// </summary>
    public const string SectionName = "RabbitMQ";

    /// <summary>
    /// 主机名，默认 localhost
    /// </summary>
    public string HostName { get; set; } = "localhost";

    /// <summary>
    /// 端口号，默认 5672
    /// </summary>
    public int Port { get; set; } = 5672;

    /// <summary>
    /// 用户名，默认 guest
    /// </summary>
    public string UserName { get; set; } = "guest";

    /// <summary>
    /// 密码，默认 guest
    /// </summary>
    public string Password { get; set; } = "guest";

    /// <summary>
    /// 虚拟主机，默认 /
    /// </summary>
    public string VirtualHost { get; set; } = "/";

    /// <summary>
    /// 连接名称（用于 RabbitMQ 管理界面识别）
    /// </summary>
    public string? ConnectionName { get; set; }

    /// <summary>
    /// 请求超时时间（秒），默认 30 秒
    /// </summary>
    public int RequestedConnectionTimeout { get; set; } = 30;

    /// <summary>
    /// 心跳间隔（秒），默认 60 秒
    /// </summary>
    public ushort RequestedHeartbeat { get; set; } = 60;

    /// <summary>
    /// 自动恢复连接，默认 true
    /// </summary>
    public bool AutomaticRecoveryEnabled { get; set; } = true;

    /// <summary>
    /// 网络恢复后自动恢复拓扑（交换机、队列等），默认 true
    /// </summary>
    public bool TopologyRecoveryEnabled { get; set; } = true;

    /// <summary>
    /// 重连间隔（毫秒），默认 5000ms
    /// </summary>
    public int NetworkRecoveryInterval { get; set; } = 5000;

    /// <summary>
    /// 通道池大小，默认 10
    /// </summary>
    public int ChannelPoolSize { get; set; } = 10;

    /// <summary>
    /// 发布确认超时时间（毫秒），默认 5000ms
    /// </summary>
    public int PublisherConfirmTimeout { get; set; } = 5000;

    /// <summary>
    /// 是否启用发布确认，默认 true
    /// </summary>
    public bool PublisherConfirmsEnabled { get; set; } = true;
}
