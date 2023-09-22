using System.Text.Json;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.DynamoDBv2.Model;
using Amazon.Lambda.Core;
using Amazon.Lambda.DynamoDBEvents;
using DocumentProcessor.Pdf.Models;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace DocumentProcessor.Pdf;

public class Function
{
    public void FunctionHandler(DynamoDBEvent dynamoEvent, ILambdaContext context)
    {
        context.Logger.LogInformation($"Beginning to process {dynamoEvent.Records.Count} records...");

        foreach (var record in dynamoEvent.Records)
        {
            context.Logger.LogInformation($"Event ID: {record.EventID}");
            context.Logger.LogInformation($"Event Name: {record.EventName}");

            var request = Deserialize(record.Dynamodb.NewImage);

        }

        context.Logger.LogInformation("Stream processing complete.");
    }

    public DocumentRequest? Deserialize(Dictionary<string, AttributeValue> image)
    {
        var json = Document.FromAttributeMap(image).ToJson(); // Make this normal JSON because DynamoDB sucks...
        return JsonSerializer.Deserialize<DocumentRequest>(json, new JsonSerializerOptions{ PropertyNameCaseInsensitive = true });
    }
}