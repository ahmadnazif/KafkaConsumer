
using Confluent.Kafka;
using static System.Net.Mime.MediaTypeNames;

namespace KafkaConsumer.Workers;

public class KafkaConsumerWorker(ILogger<KafkaConsumerWorker> logger, ConsumerConfig consumerConfig, IConfiguration config) : BackgroundService
{
    private readonly ILogger<KafkaConsumerWorker> logger = logger;
    private readonly string topic = config["Kafka:Topic"];
    private readonly ConsumerConfig consumerConfig = consumerConfig;

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        Log("Preparing consumer..");
        await Task.Delay(3000, ct);
        Log("Consumer is ready now");

        using var consumer = new ConsumerBuilder<Null, string>(consumerConfig).Build();

        Log($"Subscribing to topic {topic}..");
        consumer.Subscribe(topic);
        Log($"Subscribed");

        try
        {
            while (!ct.IsCancellationRequested)
            {
                try
                {
                    var r = consumer.Consume(ct);
                    var txt = $"Topic: {r.Topic}, Msg: {r.Message.Value}, Time: {r.Message.Timestamp.UtcDateTime.ToLocalTime()}";
                    Log($"{txt}");
                }
                catch (OperationCanceledException)
                {
                    consumer.Unsubscribe();
                    Log("Consumer cancellation requested");
                    break;
                }
                catch (ConsumeException ex)
                {
                    consumer.Unsubscribe();
                    LogError($"ErrorCode: {ex.Error?.Code}, Reason: {ex.Error.Reason}");
                    if (ex.Error.IsFatal)
                    {
                        LogError("Fatal error occured. Loop is breaked");
                        break;
                    }
                }
                catch (Exception ex)
                {
                    consumer.Unsubscribe();
                    LogError($"In-loop Exception: {ex}");
                    break;
                }
            }
        }
        catch (Exception ex)
        {
            LogError($"Exception: {ex.Message}");
        }
        finally
        {
            consumer.Close();
            consumer.Dispose();
            Log("Consumer disposed");
        }
    }

    protected async Task ExecuteAsync_WRONG(CancellationToken ct)
    {
        await Task.Delay(3000, ct);
        using IConsumer<string, string> consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();
        consumer.Subscribe(topic);

        try
        {
            while (!ct.IsCancellationRequested)
            {
                try
                {
                    var result = consumer.Consume(ct);
                    Log($"{JsonSerializer.Serialize(result)}");
                }
                catch (ConsumeException ex)
                {
                    consumer.Unsubscribe();
                    LogError($"ErrorCode: {ex.Error?.Code}, Reason: {ex.Error.Reason}");
                    if (ex.Error.IsFatal)
                    {
                        LogError("Fatal error. Breaked");
                        break;
                    }
                }
                catch (Exception ex)
                {
                    consumer.Unsubscribe();
                    LogError($"In loop Error: {ex.Message}");
                    break;
                }
            }
        }
        catch (OperationCanceledException)
        {
            consumer.Unsubscribe();
            Log("Consumer cancellation requested");
        }
        catch (Exception ex)
        {
            consumer.Unsubscribe();
            LogError($"Outer Error: {ex.Message}");
        }
        finally
        {
            consumer.Close();
            consumer.Dispose();
            Log("Consumer disposed");
        }
    }

    public override Task StopAsync(CancellationToken ct) => base.StopAsync(ct);

    private void Log(string txt) => logger.LogInformation($"{DateTime.Now.ToLongTimeString()} | {txt}");
    private void LogError(Exception ex) => logger.LogInformation($"{DateTime.Now.ToLongTimeString()} | {ex.Message}");
    private void LogError(string txt) => logger.LogInformation($"{DateTime.Now.ToLongTimeString()} | {txt}");
}
