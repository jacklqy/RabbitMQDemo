using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQWrapper.Options;

namespace RabbitMQWrapper.Connection;

/// <summary>
/// RabbitMQ 连接管理器
/// 采用单例模式管理连接，确保整个应用程序只有一个连接实例
/// 支持自动重连、连接恢复事件处理
/// </summary>
public interface IRabbitMQConnectionManager : IDisposable
{
    /// <summary>
    /// 获取连接是否已打开
    /// </summary>
    bool IsConnected { get; }

    /// <summary>
    /// 创建新通道
    /// </summary>
    IModel CreateChannel();

    /// <summary>
    /// 确保连接已建立
    /// </summary>
    void EnsureConnected();

    /// <summary>
    /// 连接恢复事件
    /// </summary>
    event EventHandler? ConnectionRecovered;

    /// <summary>
    /// 连接断开事件
    /// </summary>
    event EventHandler? ConnectionShutdown;
}

/// <summary>
/// RabbitMQ 连接管理器实现
/// </summary>
public class RabbitMQConnectionManager : IRabbitMQConnectionManager
{
    private readonly RabbitMQOptions _options;
    private readonly ILogger<RabbitMQConnectionManager> _logger;
    private readonly object _lock = new();
    private IConnection? _connection;
    private bool _disposed;

    /// <summary>
    /// 连接是否已打开
    /// </summary>
    public bool IsConnected => _connection != null && _connection.IsOpen;

    /// <summary>
    /// 连接恢复事件
    /// </summary>
    public event EventHandler? ConnectionRecovered;

    /// <summary>
    /// 连接断开事件
    /// </summary>
    public event EventHandler? ConnectionShutdown;

    /// <summary>
    /// 构造函数
    /// </summary>
    /// <param name="options">RabbitMQ 配置选项</param>
    /// <param name="logger">日志记录器</param>
    public RabbitMQConnectionManager(IOptions<RabbitMQOptions> options, ILogger<RabbitMQConnectionManager> logger)
    {
        _options = options.Value;
        _logger = logger;
    }

    /// <summary>
    /// 确保连接已建立
    /// </summary>
    public void EnsureConnected()
    {
        if (IsConnected) return;

        lock (_lock)
        {
            if (IsConnected) return;

            _logger.LogInformation("正在创建 RabbitMQ 连接...");

            var factory = new ConnectionFactory
            {
                HostName = _options.HostName,
                Port = _options.Port,
                UserName = _options.UserName,
                Password = _options.Password,
                VirtualHost = _options.VirtualHost,
                RequestedConnectionTimeout = TimeSpan.FromSeconds(_options.RequestedConnectionTimeout),
                RequestedHeartbeat = TimeSpan.FromSeconds(_options.RequestedHeartbeat),
                AutomaticRecoveryEnabled = _options.AutomaticRecoveryEnabled,
                TopologyRecoveryEnabled = _options.TopologyRecoveryEnabled,
                NetworkRecoveryInterval = TimeSpan.FromMilliseconds(_options.NetworkRecoveryInterval),
                ClientProvidedName = _options.ConnectionName ?? "RabbitMQWrapper"
            };

            try
            {
                _connection = factory.CreateConnection();

                _connection.ConnectionShutdown += OnConnectionShutdown;
                _connection.ConnectionRecoveryError += OnConnectionRecoveryError;
                _connection.CallbackException += OnCallbackException;

                if (_options.AutomaticRecoveryEnabled)
                {
                    _connection.RecoverySucceeded += OnRecoverySucceeded;
                }

                _logger.LogInformation("RabbitMQ 连接已建立，Host: {HostName}, Port: {Port}, VirtualHost: {VirtualHost}",
                    _options.HostName, _options.Port, _options.VirtualHost);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "创建 RabbitMQ 连接失败: {Message}", ex.Message);
                throw;
            }
        }
    }

    /// <summary>
    /// 创建新通道
    /// </summary>
    /// <returns>RabbitMQ 通道</returns>
    public IModel CreateChannel()
    {
        EnsureConnected();
        return _connection!.CreateModel();
    }

    private void OnConnectionShutdown(object? sender, ShutdownEventArgs e)
    {
        _logger.LogWarning("RabbitMQ 连接断开: {Reason} ({Initiator}", e.ReplyText, e.Initiator);
        ConnectionShutdown?.Invoke(this, EventArgs.Empty);
    }

    private void OnRecoverySucceeded(object? sender, EventArgs e)
    {
        _logger.LogInformation("RabbitMQ 连接已自动恢复");
        ConnectionRecovered?.Invoke(this, EventArgs.Empty);
    }

    private void OnConnectionRecoveryError(object? sender, ConnectionRecoveryErrorEventArgs e)
    {
        _logger.LogError(e.Exception, "RabbitMQ 连接恢复失败: {Message}", e.Exception.Message);
    }

    private void OnCallbackException(object? sender, CallbackExceptionEventArgs e)
    {
        _logger.LogError(e.Exception, "RabbitMQ 回调异常: {Message}", e.Exception.Message);
    }

    /// <summary>
    /// 释放资源
    /// </summary>
    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    /// <summary>
    /// 释放资源
    /// </summary>
    /// <param name="disposing">是否释放托管资源</param>
    protected virtual void Dispose(bool disposing)
    {
        if (_disposed) return;

        if (disposing)
        {
            if (_connection != null)
            {
                try
                {
                    _connection.ConnectionShutdown -= OnConnectionShutdown;
                    _connection.ConnectionRecoveryError -= OnConnectionRecoveryError;
                    _connection.CallbackException -= OnCallbackException;
                    if (_options.AutomaticRecoveryEnabled)
                    {
                        _connection.RecoverySucceeded -= OnRecoverySucceeded;
                    }
                    _connection.Close();
                    _connection.Dispose();
                    _logger.LogInformation("RabbitMQ 连接已关闭");
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "关闭 RabbitMQ 连接时发生异常");
                }
            }
        }

        _disposed = true;
    }
}
