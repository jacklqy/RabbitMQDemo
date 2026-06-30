using RabbitMQDemo.Api.Handlers;
using RabbitMQDemo.Api.Models;
using RabbitMQWrapper.DependencyInjection;

var builder = WebApplication.CreateBuilder(args);

// 添加 RabbitMQ 核心服务
builder.Services.AddRabbitMQ(builder.Configuration);

// 添加基础消费者和后台服务（订单处理）
builder.Services.AddBasicConsumerHostedService<OrderMessage, OrderMessageHandler>(options =>
{
    options.QueueName = "demo.order.queue";
    options.PrefetchCount = 1;
    options.MaxRetryCount = 3;
    options.RetryInterval = 1000;
    options.AutoAck = false;
});

// 添加邮件消费者（用户注册 - 发送邮件）
builder.Services.AddBasicConsumerHostedService<UserRegisterMessage, UserRegisterEmailHandler>(options =>
{
    options.QueueName = "demo.user.register.email";
    options.PrefetchCount = 10;
    options.MaxRetryCount = 3;
    options.AutoAck = false;
});

// 添加短信消费者（用户注册 - 发送短信）
builder.Services.AddBasicConsumerHostedService<UserRegisterMessage, UserRegisterSmsHandler>(options =>
{
    options.QueueName = "demo.user.register.sms";
    options.PrefetchCount = 10;
    options.MaxRetryCount = 3;
    options.AutoAck = false;
});

// 添加批量日志消费者
builder.Services.AddBatchConsumer<LogMessage>(options =>
{
    options.QueueName = "demo.log.queue";
    options.PrefetchCount = 100;
    options.MaxRetryCount = 3;
    options.AutoAck = false;
}, batchSize: 50, batchTimeoutMs: 3000);

builder.Services.AddScoped<LogBatchHandler>();

// 添加控制器
builder.Services.AddControllers();
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

var app = builder.Build();

if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseHttpsRedirection();
app.UseAuthorization();
app.MapControllers();

app.Run();
