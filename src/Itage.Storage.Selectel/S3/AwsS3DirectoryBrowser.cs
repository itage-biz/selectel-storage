using Amazon.S3;
using Amazon.S3.Model;
using NetBox.Async;
using Storage.Net;
using Storage.Net.Blobs;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Itage.Storage.Selectel.S3
{
    internal class AwsS3DirectoryBrowser : IDisposable
    {
        private readonly AsyncLimiter _limiter = new AsyncLimiter(10);
        private readonly AmazonS3Client _client;
        private readonly string _bucketName;

        public AwsS3DirectoryBrowser(AmazonS3Client client, string bucketName)
        {
            this._client = client;
            this._bucketName = bucketName;
        }

        public async Task<IReadOnlyCollection<Blob>> ListAsync(
            ListOptions options,
            CancellationToken cancellationToken)
        {
            List<Blob> container = new List<Blob>();
            await this.ListFolderAsync(container, options.FolderPath, options, cancellationToken).ConfigureAwait(false);
            List<Blob> blobList;
            if (options.MaxResults.HasValue)
            {
                int count1 = container.Count;
                int? maxResults = options.MaxResults;
                int num = maxResults.Value;
                if (count1 <= num)
                {
                    blobList = container;
                }
                else
                {
                    List<Blob> source = container;
                    maxResults = options.MaxResults;
                    int count2 = maxResults.Value;
                    blobList = source.Take(count2).ToList();
                }
            }
            else
                blobList = container;

            return blobList;
        }

        private async Task ListFolderAsync(
            List<Blob> container,
            string path,
            ListOptions options,
            CancellationToken cancellationToken)
        {
            ListObjectsV2Request request = new ListObjectsV2Request
            {
                BucketName = this._bucketName,
                Prefix = FormatFolderPrefix(path),
                Delimiter = "/"
            };
            List<Blob> folderContainer = new List<Blob>();
            while (true)
            {
                int? maxResults = options.MaxResults;
                if (maxResults.HasValue)
                    goto label_11;
                label_1:
                ListObjectsV2Response response;
                using (await this._limiter.AcquireOneAsync().ConfigureAwait(false))
                    response = await this._client.ListObjectsV2Async(request, cancellationToken).ConfigureAwait(false);
                folderContainer.AddRange(response.ToBlobs(options));
                if (response.NextContinuationToken != null)
                {
                    request.ContinuationToken = response.NextContinuationToken;
                    continue;
                }

                break;
                label_11:
                int count = container.Count;
                maxResults = options.MaxResults;
                int valueOrDefault = maxResults.GetValueOrDefault();
                if (count < valueOrDefault & maxResults.HasValue)
                    goto label_1;
                break;
            }

            container.AddRange(folderContainer);
            if (!options.Recurse)
                return;
            await Task.WhenAll(folderContainer.Where(b => b.Kind == BlobItemKind.Folder)
                .ToList().Select(f =>
                    this.ListFolderAsync(container, f.FullPath, options, cancellationToken))).ConfigureAwait(false);
        }

        private static string? FormatFolderPrefix(string folderPath)
        {
            folderPath = StoragePath.Normalize(folderPath);
            if (StoragePath.IsRootPath(folderPath))
                return null;
            if (!folderPath.EndsWith("/"))
                folderPath += "/";
            return folderPath.TrimStart('/');
        }

        public async Task DeleteRecursiveAsync(string fullPath, CancellationToken cancellationToken)
        {
            ListObjectsV2Request request = new ListObjectsV2Request
            {
                BucketName = this._bucketName, Prefix = fullPath + "/"
            };
            while (true)
            {
                ListObjectsV2Response response =
                    await this._client.ListObjectsV2Async(request, cancellationToken).ConfigureAwait(false);
                await Task.WhenAll(
                    response.S3Objects.Select(
                        s3 =>
                            this._client.DeleteObjectAsync(this._bucketName, s3.Key, cancellationToken)));
                if (response.NextContinuationToken != null)
                {
                    request.ContinuationToken = response.NextContinuationToken;
                }
                else
                    break;
            }
        }

        public void Dispose()
        {
            this._limiter.Dispose();
        }
    }
}