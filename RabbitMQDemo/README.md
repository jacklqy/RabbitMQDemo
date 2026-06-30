# RabbitMQWrapper 高级封装类库

基于 ASP.NET Core Web API 的高级 RabbitMQ 封装类库，提供完善的 RabbitMQ 使用体验。

## 功能特性

- ✅ **单例连接管理** - 整个应用共享一个连接，自动重连
- ✅ **ChannelPool 通道池** - 支持高并发生产者，通道复用
- ✅ **发布确认（Publisher Confirms）** - 确保消息可靠投递
- ✅ **死信队列（DLX）** - 消息失败不丢失，支持 TTL 和最大长度
- ✅ **延迟消息** - 基于死信队列 + TTL 实现延迟消息
- ✅ **多种消费模式** - 基础消费、批量消费、死信消费
- ✅ **自动重试机制** - 消费失败自动重试，支持配置重试次数和间隔
- ✅ **依赖注入** - 完美支持 ASP.NET Core DI，开箱即用
- ✅ **后台服务** - 消费者托管服务，应用启动自动运行
- ✅ **详细日志** - 完整的日志记录，便于排查问题

## 项目结构

```
RabbitMQDemo/
├── src/
│   ├── RabbitMQWrapper/          # 核心类库
│   │   ├── ChannelPool/          # 通道池
│   │   ├── Connection/           # 连接管理
│   │   ├── Consumers/            # 消费者
│   │   ├── DependencyInjection/  # 依赖注入扩展
│   │   ├── Hosting/              # 后台服务
│   │   ├── Models/               # 模型
│   │   ├── Options/              # 配置选项
│   │   └── Producer/             # 生产者
│   └── RabbitMQDemo.Api/         # Web API Demo
│       ├── Controllers/          # 演示控制器
│       ├── Handlers/             # 消息处理器
│       └── Models/               # 消息模型
```

## 快速开始

### 1. 安装 NuGet 包（或引用项目）

```bash
dotnet add reference ./RabbitMQWrapper.csproj
```

### 2. 配置 appsettings.json

```json
{
  "RabbitMQ": {
    "HostName": "localhost",
    "Port": 5672,
    "UserName": "guest",
    "Password": "guest",
    "VirtualHost": "/",
    "ConnectionName": "MyApp",
    "ChannelPoolSize": 10,
    "PublisherConfirmsEnabled": true,
    "PublisherConfirmTimeout": 5000,
    "AutomaticRecoveryEnabled": true,
    "NetworkRecoveryInterval": 5000
  }
}
```

### 3. 注册服务

```csharp
using RabbitMQWrapper.DependencyInjection;

var builder = WebApplication.CreateBuilder(args);

// 注册 RabbitMQ 核心服务
builder.Services.AddRabbitMQ(builder.Configuration);
```

## 使用场景

### 场景一：基础消息发布/消费

#### 定义消息模型

```csharp
public class OrderMessage
{
    public string OrderId { get; set; }
    public string OrderName { get; set; }
    public decimal Amount { get; set; }
}
```

#### 实现消息处理器

```csharp
public class OrderMessageHandler : IMessageHandler<OrderMessage>
{
    private readonly ILogger<OrderMessageHandler> _logger;

    public OrderMessageHandler(ILogger<OrderMessageHandler> logger)
    {
        _logger = logger;
    }

    public Task<MessageProcessResult> HandleAsync(MessageContext<OrderMessage> context)
    {
        var order = context.Body;
        _logger.LogInformation("处理订单: {OrderId}", order.OrderId);

        // 业务逻辑...

        return Task.FromResult(MessageProcessResult.Success);
    }
}
```

#### 注册消费者后台服务

```csharp
builder.Services.AddBasicConsumerHostedService<OrderMessage, OrderMessageHandler>(options =>
{
    options.QueueName = "order.queue";
    options.PrefetchCount = 1;
    options.MaxRetryCount = 3;
    options.RetryInterval = 1000;
    options.AutoAck = false;
});
```

#### 发布消息

```csharp
public class OrderController : ControllerBase
{
    private readonly IRabbitMQProducer _producer;

    public OrderController(IRabbitMQProducer producer)
    {
        _producer = producer;
    }

    [HttpPost]
    public IActionResult CreateOrder([FromBody] OrderMessage order)
    {
        var result = _producer.Publish("order.queue", order);
        return Ok(result);
    }
}
```

### 场景二：发布/订阅模式（交换机）

#### 发布消息到交换机

```csharp
// Direct 模式
var result = _producer.PublishToExchange(
    exchangeName: "user.exchange",
    routingKey: "email",
    message: userMessage,
    exchangeType: "direct");

// Fanout 模式（广播）
var result = _producer.PublishToExchange(
    exchangeName: "broadcast.exchange",
    routingKey: string.Empty,
    message: broadcastMessage,
    exchangeType: "fanout");

// Topic 模式（通配符）
var result = _producer.PublishToExchange(
    exchangeName: "topic.exchange",
    routingKey: "order.created",
    message: orderMessage,
    exchangeType: "topic");
```

### 场景三：死信队列

#### 发布带死信队列的消息

```csharp
var deadLetterOptions = new DeadLetterOptions
{
    Enabled = true,
    DeadLetterExchange = "order.dlx",
    DeadLetterRoutingKey = "order.deadletter",
    DeadLetterQueueName = "order.deadletter.queue",
    MessageTtl = 30000,  // 消息30秒未消费进入死信
    MaxLength = 10000   // 队列最大长度
};

var result = _producer.PublishWithDeadLetter("order.queue", orderMessage, deadLetterOptions);
```

#### 消费死信队列

```csharp
// 注册死信消费者
builder.Services.AddDeadLetterConsumer<OrderMessage>("order.deadletter.queue");

// 手动启动消费
var consumer = serviceProvider.GetRequiredService<DeadLetterConsumer<OrderMessage>>();
consumer.OnDeadLetterReceived = HandleDeadLetterAsync;
consumer.Start();
```

### 场景四：延迟消息

```csharp
// 发布延迟消息（延迟30分钟）
var result = _producer.PublishDelayed(
    queueName: "order.timeout.queue",
    message: orderMessage,
    delayMilliseconds: 30 * 60 * 1000);
```

**使用场景：**
- 订单超时取消
- 延迟通知
- 定时任务
- 重试延迟

### 场景五：批量消费

```csharp
// 注册批量消费者
builder.Services.AddBatchConsumer<LogMessage>(options =>
{
    options.QueueName = "log.queue";
    options.PrefetchCount = 100;
}, batchSize: 50, batchTimeoutMs: 3000);

// 使用批量消费者
var consumer = serviceProvider.GetRequiredService<BatchConsumer<LogMessage>>();
consumer.OnBatchMessageReceived = HandleBatchAsync;
consumer.Start();

private Task<MessageProcessResult> HandleBatchAsync(List<MessageContext<LogMessage>> messages)
{
    // 批量处理消息
    // 例如：批量写入数据库
    return Task.FromResult(MessageProcessResult.Success);
}
```

### 场景六：高并发发布

```csharp
// 通道池自动管理，无需额外配置
// 直接使用生产者即可支持高并发
Parallel.For(0, 1000, i =>
{
    var message = new OrderMessage { OrderId = $"ORDER-{i}" };
    var result = _producer.Publish("concurrent.queue", message);
});
```

### 场景七：手动管理消费者

```csharp
public class MyService
{
    private readonly IConsumer<OrderMessage> _consumer;

    public MyService(IConsumer<OrderMessage> consumer)
    {
        _consumer = consumer;
    }

    public void Start()
    {
        _consumer.OnMessageReceived = HandleMessageAsync;
        _consumer.Start();
    }

    public void Stop()
    {
        _consumer.Stop();
    }

    private Task<MessageProcessResult> HandleMessageAsync(MessageContext<OrderMessage> context)
    {
        // 处理消息
        return Task.FromResult(MessageProcessResult.Success);
    }
}
```

## 消息处理结果

| 结果 | 说明 |
|------|------|
| `MessageProcessResult.Success` | 处理成功，确认消息 |
| `MessageProcessResult.Retry` | 处理失败，重试消息 |
| `MessageProcessResult.Reject` | 处理失败，拒绝消息（进入死信或丢弃） |

## 配置说明

### RabbitMQOptions

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| HostName | string | localhost | 主机名 |
| Port | int | 5672 | 端口号 |
| UserName | string | guest | 用户名 |
| Password | string | guest | 密码 |
| VirtualHost | string | / | 虚拟主机 |
| ConnectionName | string | null | 连接名称 |
| ChannelPoolSize | int | 10 | 通道池大小 |
| PublisherConfirmsEnabled | bool | true | 是否启用发布确认 |
| PublisherConfirmTimeout | int | 5000 | 发布确认超时(ms) |
| AutomaticRecoveryEnabled | bool | true | 自动重连 |
| NetworkRecoveryInterval | int | 5000 | 重连间隔(ms) |

### ConsumerOptions

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| QueueName | string | - | 队列名称 |
| PrefetchCount | ushort | 1 | 预取消息数 |
| MaxRetryCount | int | 3 | 最大重试次数 |
| RetryInterval | int | 1000 | 重试间隔(ms) |
| AutoAck | bool | false | 自动确认 |
| ConsumerCount | int | 1 | 消费者数量 |

## 运行 Demo

### 前置条件

- .NET 6.0 SDK
- RabbitMQ 服务（本地或远程）

### 启动步骤

1. 启动 RabbitMQ 服务
2. 修改 `appsettings.json` 中的 RabbitMQ 连接配置
3. 运行 Web API：

```bash
cd src/RabbitMQDemo.Api
dotnet run
```

4. 打开 Swagger UI：`http://localhost:5000/swagger`

### Demo API 列表

| API | 说明 |
|-----|------|
| POST /api/RabbitMQDemo/basic/simple | 发布简单消息 |
| POST /api/RabbitMQDemo/basic/batch/{count} | 批量发布消息 |
| POST /api/RabbitMQDemo/exchange/direct | Direct 交换机 |
| POST /api/RabbitMQDemo/exchange/fanout | Fanout 交换机 |
| POST /api/RabbitMQDemo/exchange/topic | Topic 交换机 |
| POST /api/RabbitMQDemo/deadletter | 死信队列 |
| POST /api/RabbitMQDemo/deadletter/ttl | TTL 死信 |
| POST /api/RabbitMQDemo/delayed | 延迟消息 |
| POST /api/RabbitMQDemo/concurrent | 高并发测试 |
| POST /api/ConsumerDemo/manual/start | 启动手动消费者 |
| POST /api/ConsumerDemo/manual/stop | 停止手动消费者 |
| POST /api/ConsumerDemo/deadletter/start | 启动死信消费者 |
| POST /api/ConsumerDemo/deadletter/stop | 停止死信消费者 |

## 核心组件说明

### IRabbitMQConnectionManager

连接管理器，采用单例模式管理 RabbitMQ 连接。

- 整个应用共享一个连接
- 自动重连机制
- 连接状态监控
- 事件通知（连接恢复、连接断开）

### IChannelPool

通道池，管理和复用 RabbitMQ 通道。

- 避免频繁创建销毁通道
- 支持高并发场景
- 自动检测并替换失效通道
- 可配置池大小

### IRabbitMQProducer

生产者，提供多种消息发布方式。

- 简单队列发布
- 交换机模式发布
- 死信队列支持
- 延迟消息
- 批量发布
- 发布确认机制

### 消费者系列

- **BasicConsumer** - 基础消费者，单条消息处理
- **BatchConsumer** - 批量消费者，批量处理消息
- **DeadLetterConsumer** - 死信消费者，处理死信消息
- **ConsumerBackgroundService** - 后台服务，自动启动消费

## 最佳实践

1. **使用发布确认** - 确保消息可靠投递
2. **手动确认消息** - AutoAck = false，处理完再确认
3. **合理设置 PrefetchCount** - 根据消费能力调整
4. **使用死信队列** - 避免消息丢失
5. **通道池调优** - 根据并发量调整 ChannelPoolSize
6. **异常处理** - 消费异常要捕获，避免消费者崩溃
7. **日志记录** - 关键操作都要记录日志
8. **优雅停机** - 应用关闭时停止消费者

## License

MIT
