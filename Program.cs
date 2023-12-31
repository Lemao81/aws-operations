using Amazon.Extensions.NETCore.Setup;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.S3.Util;
using Amazon.SimpleNotificationService;
using Amazon.SimpleNotificationService.Model;
using AWSOperations.Consts;
using Microsoft.AspNetCore.Mvc;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddDefaultAWSOptions(builder.Configuration.GetAWSOptions());
builder.Services.AddAWSService<IAmazonS3>();
builder.Services.AddAWSService<IAmazonSimpleNotificationService>();

var app = builder.Build();

app.MapPost("buckets/{bucket}", async ([FromRoute] string bucket, [FromServices] IAmazonS3 s3Client, [FromServices] IAmazonSimpleNotificationService snsClient,
    [FromServices] IConfiguration configuration, CancellationToken ct) =>
{
    try
    {
        if (await AmazonS3Util.DoesS3BucketExistV2Async(s3Client, bucket))
        {
            return Results.Ok("Bucket exists already");
        }

        await s3Client.PutBucketAsync(bucket, ct);

        var arnBase = configuration["SnsArnBase"] ?? throw new ConfigurationException("Missing sns arn base configuration");
        var publishRequest = new PublishRequest
        {
            Message = $"Bucket {bucket} has been created",
            TopicArn = arnBase + SnsTopics.S3BucketCreated
        };

        await snsClient.PublishAsync(publishRequest, ct);

        return Results.Created();
    }
    catch (Exception exception)
    {
        Console.WriteLine(exception);

        return Results.Problem(exception.Message);
    }
});

app.MapPost("buckets/{bucket}/store", async ([FromRoute] string bucket, [FromForm] IFormFile file, [FromServices] IAmazonS3 s3Client,
    [FromServices] IAmazonSimpleNotificationService snsClient, [FromServices] IConfiguration configuration, CancellationToken ct) =>
{
    try
    {
        if (!await AmazonS3Util.DoesS3BucketExistV2Async(s3Client, bucket))
        {
            return Results.NotFound($"Bucket {bucket} not found");
        }

        var putRequest = new PutObjectRequest
        {
            BucketName = bucket,
            Key = file.FileName,
            InputStream = file.OpenReadStream()
        };

        putRequest.Metadata.Add("Content-Type", file.ContentType);

        await s3Client.PutObjectAsync(putRequest, ct);

        var arnBase = configuration["SnsArnBase"] ?? throw new ConfigurationException("Missing sns arn base configuration");
        var publishRequest = new PublishRequest
        {
            Message = $"Object {putRequest.Key} added to bucket {bucket}",
            TopicArn = arnBase + SnsTopics.S3ObjectPut
        };

        await snsClient.PublishAsync(publishRequest, ct);

        return Results.Ok();
    }
    catch (Exception exception)
    {
        Console.WriteLine(exception);

        return Results.Problem(exception.Message);
    }
}).DisableAntiforgery();

app.MapDelete("buckets/{bucket}", async ([FromRoute] string bucket, [FromServices] IAmazonS3 s3Client, CancellationToken ct) =>
{
    try
    {
        if (!await AmazonS3Util.DoesS3BucketExistV2Async(s3Client, bucket))
        {
            return Results.NotFound($"Bucket {bucket} not found");
        }

        var objectsResponse = await s3Client.ListObjectsAsync(bucket, ct);
        foreach (var s3Object in objectsResponse.S3Objects)
        {
            await s3Client.DeleteObjectAsync(bucket, s3Object.Key, ct);
        }

        var request = new DeleteBucketRequest { BucketName = bucket };

        await s3Client.DeleteBucketAsync(request, ct);

        return Results.NoContent();
    }
    catch (Exception exception)
    {
        Console.WriteLine(exception);

        return Results.Problem(exception.Message);
    }
});

app.Run();
