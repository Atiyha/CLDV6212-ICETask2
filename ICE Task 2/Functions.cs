using System.Text.Json;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace ICE_Task_2;

public class IncomingMessage
{
    public string Name { get; set; } = "";
    public string Message { get; set; } = "";
}

public class MessageEntity
{
    public string PartitionKey { get; set; } = "Messages";
    public string RowKey { get; set; } = Guid.NewGuid().ToString();
    public string Name { get; set; } = "";
    public string Message { get; set; } = "";
    public DateTime ReceivedUtc { get; set; } = DateTime.UtcNow;
}

public class HttpToQueueOutput
{
    [QueueOutput("incoming-messages", Connection = "AzureWebJobsStorage")]
    public string? QueueMessage { get; set; }

    [HttpResult]
    public IActionResult HttpResponse { get; set; } = default!;
}

public class Functions
{
    private readonly ILogger<Functions> _logger;
    private static readonly JsonSerializerOptions JsonOptions = new() { PropertyNameCaseInsensitive = true };

    public Functions(ILogger<Functions> logger) => _logger = logger;

    // STEP 1: HTTP trigger (Postman) -> writes to queue
    [Function("HttpToQueue")]
    public async Task<HttpToQueueOutput> HttpToQueue(
        [HttpTrigger(AuthorizationLevel.Function, "post")] HttpRequest req)
    {
        IncomingMessage? msg = null;

        try
        {
            msg = await JsonSerializer.DeserializeAsync<IncomingMessage>(req.Body, JsonOptions);
        }
        catch (JsonException) { }

        if (msg == null || string.IsNullOrWhiteSpace(msg.Name) || string.IsNullOrWhiteSpace(msg.Message))
        {
            return new HttpToQueueOutput
            {
                HttpResponse = new BadRequestObjectResult("Send JSON like {\"name\":\"Sam\",\"message\":\"Hello\"}")
            };
        }

        _logger.LogInformation("Queuing message from {Name}", msg.Name);

        return new HttpToQueueOutput
        {
            QueueMessage = JsonSerializer.Serialize(msg),
            HttpResponse = new OkObjectResult("Message added to queue.")
        };
    }

    [Function("QueueToTable")]
    [TableOutput("Messages", Connection = "AzureWebJobsStorage")]
    public MessageEntity QueueToTable(
        [QueueTrigger("incoming-messages", Connection = "AzureWebJobsStorage")] string queueItem)
    {
        _logger.LogInformation("Dequeued: {Item}", queueItem);

        var msg = JsonSerializer.Deserialize<IncomingMessage>(queueItem, JsonOptions)!;

        return new MessageEntity
        {
            Name = msg.Name,
            Message = msg.Message
        };
    }
}