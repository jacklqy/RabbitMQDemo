using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQWrapper.Connection;
using RabbitMQWrapper.Options;

namespace RabbitMQWrapper.ChannelPool;

/// <summary>
/// RabbitMQ 通道池接口
/// 用于管理和复用通道，提高高并发场景下的性能
/// 避免频繁创建和销毁通道带来的开销
/// </summary>
public interface IChannelPool : IDisposable
{
    /// <summary>
    /// 从池中获取一个通道
    /// </summary>
    /// <returns>RabbitMQ 通道</returns>
    IModel GetChannel();

    /// <summary>
    /// 将通道归还到池中
    /// </summary>
    /// <param name="channel">要归还的通道</param>
    void ReturnChannel(IModel channel);

    /// <summary>
    /// 使用通道执行操作（自动获取和归还）
    /// </summary>
    /// <typeparam name="T">返回值类型</typeparam>
    /// <param name="action">要执行的操作</param>
    /// <returns>操作结果</returns>
    T UseChannel<T>(Func<IModel, T> action);

    /// <summary>
    /// 使用通道执行操作（自动获取和归还）
    /// </summary>
    /// <param name="action">要执行的操作</param>
    void UseChannel(Action<IModel> action);
}

/// <summary>
/// RabbitMQ 通道池实现
/// 使用 ConcurrentQueue 存储可用通道，支持高并发场景
/// </summary>
public class ChannelPool : IChannelPool
{
    private readonly IRabbitMQConnectionManager _connectionManager;
    private readonly RabbitMQOptions _options;
    private readonly ILogger<ChannelPool> _logger;
    private readonly ConcurrentQueue<IModel> _channels = new();
    private readonly object _lock = new();
    private int _totalChannels;
    private bool _disposed;

    /// <summary>
    /// 构造函数
    /// </summary>
    /// <param name="connectionManager">连接管理器</param>
    /// <param name="options">RabbitMQ 配置选项</param>
    /// <param name="logger">日志记录器</param>
    public ChannelPool(IRabbitMQConnectionManager connectionManager, IOptions<RabbitMQOptions> options, ILogger<ChannelPool> logger)
    {
        _connectionManager = connectionManager;
        _options = options.Value;
        _logger = logger;
    }

    /// <summary>
    /// 从池中获取一个通道
    /// 如果池中有可用通道则直接返回，否则创建新通道（未达到池大小上限时）
    /// </summary>
    /// <returns>RabbitMQ 通道</returns>
    public IModel GetChannel()
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(ChannelPool));

        if (_channels.TryDequeue(out var channel) && channel.IsOpen)
        {
            return channel;
        }

        if (channel != null && !channel.IsOpen)
        {
            _logger.LogWarning("通道已关闭，创建新通道");
            channel.Dispose();
            Interlocked.Decrement(ref _totalChannels);
        }

        lock (_lock)
        {
            if (_totalChannels < _options.ChannelPoolSize)
            {
                channel = CreateChannel();
                Interlocked.Increment(ref _totalChannels);
                _logger.LogDebug("创建新通道，当前通道总数: {TotalChannels}", _totalChannels);
                return channel;
            }
        }

        while (true)
        {
            if (_channels.TryDequeue(out channel) && channel.IsOpen)
            {
                return channel;
            }

            if (channel != null && !channel.IsOpen)
            {
                _logger.LogWarning("池中的通道已关闭，创建新通道替换");
                channel.Dispose();
                Interlocked.Decrement(ref _totalChannels);

                lock (_lock)
                {
                    if (_totalChannels < _options.ChannelPoolSize)
                    {
                        channel = CreateChannel();
                        Interlocked.Increment(ref _totalChannels);
                        return channel;
                    }
                }
            }

            Thread.Sleep(10);
        }
    }

    /// <summary>
    /// 将通道归还到池中
    /// 如果通道已关闭则不归还，直接释放
    /// </summary>
    /// <param name="channel">要归还的通道</param>
    public void ReturnChannel(IModel channel)
    {
        if (_disposed)
        {
            channel?.Dispose();
            return;
        }

        if (channel == null) return;

        if (!channel.IsOpen)
        {
            _logger.LogWarning("归还的通道已关闭，直接释放");
            channel.Dispose();
            Interlocked.Decrement(ref _totalChannels);
            return;
        }

        _channels.Enqueue(channel);
    }

    /// <summary>
    /// 使用通道执行操作（带返回值）
    /// 自动从池中获取通道，执行完毕后归还
    /// </summary>
    /// <typeparam name="T">返回值类型</typeparam>
    /// <param name="action">要执行的操作</param>
    /// <returns>操作结果</returns>
    public T UseChannel<T>(Func<IModel, T> action)
    {
        var channel = GetChannel();
        try
        {
            return action(channel);
        }
        finally
        {
            ReturnChannel(channel);
        }
    }

    /// <summary>
    /// 使用通道执行操作（无返回值）
    /// 自动从池中获取通道，执行完毕后归还
    /// </summary>
    /// <param name="action">要执行的操作</param>
    public void UseChannel(Action<IModel> action)
    {
        var channel = GetChannel();
        try
        {
            action(channel);
        }
        finally
        {
            ReturnChannel(channel);
        }
    }

    private IModel CreateChannel()
    {
        _connectionManager.EnsureConnected();
        var channel = _connectionManager.CreateChannel();
        _logger.LogDebug("通道已创建");
        return channel;
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
            while (_channels.TryDequeue(out var channel))
            {
                try
                {
                    channel.Close();
                    channel.Dispose();
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "释放通道时发生异常");
                }
            }

            _totalChannels = 0;
            _logger.LogInformation("通道池已释放");
        }

        _disposed = true;
    }
}
