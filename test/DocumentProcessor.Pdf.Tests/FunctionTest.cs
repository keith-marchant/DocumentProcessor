using Xunit;
using Amazon.Lambda.DynamoDBEvents;
using Amazon.Lambda.TestUtilities;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using DocumentProcessor.Pdf.Models;
using FluentAssertions;


namespace DocumentProcessor.Pdf.Tests;

public class FunctionTest
{
    [Fact]
    public async Task TestFunction()
    {
        // Arrange
        var @event = GetEvent();
        var context = new TestLambdaContext();
        var function = new Function();
        
        // Act
        await function.FunctionHandler(@event, context);

        // Assert
        var testLogger = context.Logger as TestLambdaLogger;
        Assert.Contains("Stream processing complete", testLogger?.Buffer.ToString());
    }

    [Fact]
    public void DynamoDB_Deserializes_Correctly()
    {
        // Arrange
        var @event = GetEvent();
        var function = new Function();
        var expected = new DocumentRequest(
            "b11a1261-4622-412a-952b-dce121c0eb0c",
            new List<DocumentFile>(),
            new List<DocumentUrl>
            {
                new DocumentUrl(DocumentType.DocX,
                    "https://file-examples.com/wp-content/storage/2017/02/file-sample_100kB.docx")
            });
        
        // Act
        var actual = function.Deserialize(@event.Records[0].Dynamodb.NewImage);

        // Assert
        actual.Should().BeEquivalentTo(expected);
    }

    private static DynamoDBEvent GetEvent()
    {
        return new DynamoDBEvent
        {
            Records = new List<DynamoDBEvent.DynamodbStreamRecord>
            {
                new DynamoDBEvent.DynamodbStreamRecord
                {
                    AwsRegion = "ap-southeast-2",
                    Dynamodb = new StreamRecord
                    {
                        ApproximateCreationDateTime = DateTime.Now,
                        Keys = new Dictionary<string, AttributeValue> { {"requestId", new AttributeValue { S = "b11a1261-4622-412a-952b-dce121c0eb0c" } } },
                        NewImage = new Dictionary<string, AttributeValue>
                        {
                            { "requestId", new AttributeValue { S = "b11a1261-4622-412a-952b-dce121c0eb0c" } },
                            { "urls", new AttributeValue { L = new List<AttributeValue>
                            {
                                new AttributeValue
                                {
                                    M = new Dictionary<string, AttributeValue>
                                    {
                                        { "type", new AttributeValue{ N = "2" } },
                                        { "url", new AttributeValue{ S = "https://file-examples.com/wp-content/storage/2017/02/file-sample_100kB.docx" } }
                                    }
                                }
                            } } },
                            { "files", new AttributeValue{ L = new List<AttributeValue>() } }
                        },
                        StreamViewType = StreamViewType.NEW_IMAGE
                    }
                }
            }
        };
    }
}