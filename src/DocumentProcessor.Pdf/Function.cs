using System.Text.Json;
using Amazon;
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
    private static IAmazonS3 _s3Client = new AmazonS3Client(BucketRegion);
    private static readonly string BucketName = "kmarchant-document-manager-test";
    
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
                await _s3Client.UploadObjectFromStreamAsync(BucketName, request.RequestId, pdfStream, null);
            }
        }

        context.Logger.LogInformation("Stream processing complete.");
    }

    public DocumentRequest? Deserialize(Dictionary<string, AttributeValue> image)
    {
        var json = Document.FromAttributeMap(image).ToJson(); // Make this normal JSON because DynamoDB sucks...
        return JsonSerializer.Deserialize<DocumentRequest>(json, new JsonSerializerOptions{ PropertyNameCaseInsensitive = true });
    }
}