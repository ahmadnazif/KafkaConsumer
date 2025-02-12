global using System.Text.Json;
global using KafkaConsumer.Models;
using Confluent.Kafka;
using KafkaConsumer.Workers;

var builder = WebApplication.CreateBuilder(args);
var config = builder.Configuration;

builder.Services.AddSingleton(sp =>
{
    ConsumerConfig clientConfig = new()
    {
        BootstrapServers = config["Kafka:BootstrapServers"],
        GroupId = config["Kafka:GroupId"],
        AutoOffsetReset = AutoOffsetReset.Earliest
    };

    return clientConfig;
});

builder.Services.AddHostedService<ConsumerWorker>();

builder.Services.AddControllers();
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

builder.Services.AddCors(x => x.AddDefaultPolicy(y => y.AllowAnyOrigin().AllowAnyHeader().AllowAnyMethod()));

builder.WebHost.ConfigureKestrel(x =>
{
    var port = int.Parse(config["Port"]);
    x.ListenAnyIP(port);
});

var app = builder.Build();
app.UseSwagger();
app.UseSwaggerUI();

app.UseAuthorization();

app.MapControllers();

await app.RunAsync();
