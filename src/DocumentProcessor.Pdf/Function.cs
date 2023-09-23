using System.Text.Json;
using Amazon;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.DynamoDBv2.Model;
using Amazon.Lambda.Core;
using Amazon.Lambda.DynamoDBEvents;
using Amazon.S3;
using Aspose.Words.Saving;
using DocumentProcessor.Pdf.Models;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace DocumentProcessor.Pdf;

public class Function
{
    // TODO: DI this
    private static readonly HttpClient DownloadClient = new HttpClient();
    private static readonly RegionEndpoint BucketRegion = RegionEndpoint.APSoutheast2;
    private static readonly IAmazonS3 S3Client = new AmazonS3Client(BucketRegion);
    private static readonly AmazonDynamoDBClient DbClient = new AmazonDynamoDBClient();
    private const string BucketName = "kmarchant-document-manager-test";
    private const string TableName = "tbl_pdf_requests";

    public async Task FunctionHandler(DynamoDBEvent dynamoEvent, ILambdaContext context)
    {
        context.Logger.LogInformation($"Beginning to process {dynamoEvent.Records.Count} records...");

        foreach (var record in dynamoEvent.Records)
        {
            context.Logger.LogInformation($"Event ID: {record.EventID}");
            context.Logger.LogInformation($"Event Name: {record.EventName}");

            var request = Deserialize(record.Dynamodb.NewImage);

            if (request is null)
            {
                throw new InvalidDataException("Unable to read dynamoEvent");
            }
            
            try
            {
                await UpdateDbStatus(request.RequestId, DocumentRequestStatus.Processing);

                foreach (var url in request.Urls)
                {
                    // TODO: Parallelize
                    await using var responseStream = await DownloadClient.GetStreamAsync(url.Url);
                    using var documentStream = new MemoryStream();
                    await responseStream.CopyToAsync(documentStream);
                    // TODO: Refactor to separate method and handle different file types
                    var doc = new Aspose.Words.Document(documentStream);
                    using var pdfStream = new MemoryStream();
                    var pdfSaveOptions = new PdfSaveOptions { DisplayDocTitle = true };
                    doc.Save(pdfStream, pdfSaveOptions);
                    // TODO: Work out combining documents
                    // TODO: Use a better naming scheme
                    await S3Client.UploadObjectFromStreamAsync(BucketName, request.RequestId, pdfStream, null);
                    await UpdateDbStatus(request.RequestId, DocumentRequestStatus.Complete);
                }
            }
            catch (Exception ex)
            {
                await UpdateDbStatus(request.RequestId, DocumentRequestStatus.Error);
            }
        }

        context.Logger.LogInformation("Stream processing complete.");
    }

    private async Task UpdateDbStatus(string requestId, DocumentRequestStatus newStatus, string error = "")
    {
        var updateRequest = new UpdateItemRequest
        {
            TableName = TableName,
            Key = new Dictionary<string, AttributeValue>{ { "requestId", new AttributeValue{ S = requestId } } },
            ExpressionAttributeNames = new Dictionary<string, string>
            {
                { "#S", "status" },
            },
            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
            {
                { ":status", new AttributeValue { N = $"{(int)newStatus}"} },    
            },
            UpdateExpression = "SET #S = :status",
        };

        await DbClient.UpdateItemAsync(updateRequest);
    }

    public DocumentRequest? Deserialize(Dictionary<string, AttributeValue> image)
    {
        var json = Document.FromAttributeMap(image).ToJson(); // Make this normal JSON because DynamoDB sucks...
        return JsonSerializer.Deserialize<DocumentRequest>(json, new JsonSerializerOptions{ PropertyNameCaseInsensitive = true });
    }
}