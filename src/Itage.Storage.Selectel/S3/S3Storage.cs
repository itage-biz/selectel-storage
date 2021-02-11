using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using NetBox.Extensions;
using Storage.Net;
using Storage.Net.Blobs;
using Storage.Net.Streaming;

namespace Egw.SelectelClient.S3
{
    /// <summary>Amazon S3 storage adapter for blobs</summary>
    internal class SelectelAwsS3BlobStorage : IBlobStorage
    {
        private readonly string _bucketName;
        private readonly AmazonS3Client _client;
        private bool _initialised;


        /// <summary>
        /// Creates a new instance of <see cref="T:Storage.Net.AwsS3BlobStorage" /> for a given S3 client configuration
        /// </summary>
        public SelectelAwsS3BlobStorage(
            string accessKeyId,
            string secretAccessKey,
            string bucketName,
            AmazonS3Config clientConfig)
        {
            if (accessKeyId == null)
                throw new ArgumentNullException(nameof(accessKeyId));
            if (secretAccessKey == null)
                throw new ArgumentNullException(nameof(secretAccessKey));
            this._bucketName = bucketName;
            this._client =
                new AmazonS3Client(
                    new BasicAWSCredentials(accessKeyId, secretAccessKey),
                    clientConfig);
        }

        private async Task<AmazonS3Client> GetClientAsync()
        {
            if (!this._initialised)
            {
                try
                {
                    await this._client.PutBucketAsync(new PutBucketRequest {BucketName = this._bucketName});
                    this._initialised = true;
                }
                catch (AmazonS3Exception ex) when (ex.ErrorCode == "BucketAlreadyOwnedByYou" ||
                                                   (ex.ErrorCode == "Conflict"))
                {
                    this._initialised = true;
                }
            }

            return this._client;
        }

        /// <summary>
        /// Lists all buckets, optionally filtering by prefix. Prefix filtering happens on client side.
        /// </summary>
        public async Task<IReadOnlyCollection<Blob>> ListAsync(
            ListOptions? options = null,
            CancellationToken cancellationToken = default)
        {
            options ??= new ListOptions();
            GenericValidation.CheckBlobPrefix(options.FilePrefix);
            AmazonS3Client client = await this.GetClientAsync().ConfigureAwait(false);
            IReadOnlyCollection<Blob> blobs;
            using (AwsS3DirectoryBrowser browser = new AwsS3DirectoryBrowser(client, this._bucketName))
                blobs = await browser.ListAsync(options, cancellationToken).ConfigureAwait(false);
            if (options.IncludeAttributes)
            {
                foreach (IEnumerable<Blob> blobs1 in blobs.Where(b => !b.IsFolder)
                    .Chunk(10))
                    await Converter.AppendMetadataAsync(client, this._bucketName, blobs1, cancellationToken)
                        .ConfigureAwait(false);
            }

            return blobs;
        }

        /// <summary>
        /// S3 doesnt support this natively and will cache everything in MemoryStream until disposed.
        /// </summary>
        public async Task WriteAsync(
            string fullPath,
            Stream dataStream,
            bool append = false,
            CancellationToken cancellationToken = default)
        {
            if (append)
                throw new NotSupportedException();
            GenericValidation.CheckBlobFullPath(fullPath);
            fullPath = StoragePath.Normalize(fullPath);
            await _client
                .PutObjectAsync(
                    new PutObjectRequest
                    {
                        BucketName = this._bucketName,
                        Key = fullPath,
                        InputStream = dataStream,
                        UseChunkEncoding = false,
                        ServerSideEncryptionMethod = ServerSideEncryptionMethod.None
                    }, cancellationToken)
                .ConfigureAwait(false);
        }

        public async Task<Stream?> OpenReadAsync(
            string fullPath,
            CancellationToken cancellationToken = default)
        {
            GenericValidation.CheckBlobFullPath(fullPath);
            fullPath = StoragePath.Normalize(fullPath);
            GetObjectResponse? getObjectResponse = await this.GetObjectAsync(fullPath).ConfigureAwait(false);
            return getObjectResponse != null
                ? new FixedStream(getObjectResponse.ResponseStream, getObjectResponse.ContentLength,
                    (Action<FixedStream>)null!)
                : (Stream)null!;
        }

        public async Task DeleteAsync(
            IEnumerable<string> fullPaths,
            CancellationToken cancellationToken = default)
        {
            AmazonS3Client client = await this.GetClientAsync().ConfigureAwait(false);
            await Task.WhenAll(fullPaths.Select(fullPath =>
                this.DeleteAsync(fullPath, client, cancellationToken))).ConfigureAwait(false);
        }

        private async Task DeleteAsync(
            string fullPath,
            AmazonS3Client client,
            CancellationToken cancellationToken = default)
        {
            GenericValidation.CheckBlobFullPath(fullPath);
            fullPath = StoragePath.Normalize(fullPath);
            await client.DeleteObjectAsync(this._bucketName, fullPath, cancellationToken).ConfigureAwait(false);
            using AwsS3DirectoryBrowser browser = new AwsS3DirectoryBrowser(client, this._bucketName);
            await browser.DeleteRecursiveAsync(fullPath, cancellationToken).ConfigureAwait(false);
        }

        public async Task<IReadOnlyCollection<bool>> ExistsAsync(
            IEnumerable<string> fullPaths,
            CancellationToken cancellationToken = default)
        {
            AmazonS3Client client = await this.GetClientAsync().ConfigureAwait(false);
            return await Task
                .WhenAll(fullPaths.Select(
                    fullPath => this.ExistsAsync(client, fullPath, cancellationToken)))
                .ConfigureAwait(false);
        }

        private async Task<bool> ExistsAsync(
            AmazonS3Client client,
            string fullPath,
            CancellationToken cancellationToken)
        {
            GenericValidation.CheckBlobFullPath(fullPath);
            try
            {
                fullPath = StoragePath.Normalize(fullPath);
                await client.GetObjectMetadataAsync(this._bucketName, fullPath, cancellationToken)
                    .ConfigureAwait(false);
                return true;
            }
            catch (AmazonS3Exception ex) when (ex.StatusCode == HttpStatusCode.NotFound)
            {
            }

            return false;
        }

        public async Task<IReadOnlyCollection<Blob?>> GetBlobsAsync(
            IEnumerable<string> fullPaths,
            CancellationToken cancellationToken = default)
        {
            SelectelAwsS3BlobStorage awsS3BlobStorage = this;
            var result = await Task.WhenAll(
                    fullPaths.Select(awsS3BlobStorage.GetBlobAsync))
                .ConfigureAwait(false);
            return result;
        }

        private async Task<Blob?> GetBlobAsync(string fullPath)
        {
            GenericValidation.CheckBlobFullPath(fullPath);
            fullPath = StoragePath.Normalize(fullPath);
            AmazonS3Client amazonS3Client = await this.GetClientAsync().ConfigureAwait(false);
            try
            {
                return (await amazonS3Client.GetObjectMetadataAsync(this._bucketName, fullPath)
                    .ConfigureAwait(false)).ToBlob(fullPath);
            }
            catch (AmazonS3Exception ex) when (ex.StatusCode == HttpStatusCode.NotFound)
            {
            }

            return null;
        }

        public async Task SetBlobsAsync(
            IEnumerable<Blob> blobs,
            CancellationToken cancellationToken = default)
        {
            AmazonS3Client client = await this.GetClientAsync().ConfigureAwait(false);
            foreach (Blob blob in blobs.Where(b => b != null))
            {
                if (blob.Metadata != null)
                    await Converter.UpdateMetadataAsync(client, blob, this._bucketName, blob)
                        .ConfigureAwait(false);
            }
        }

        private async Task<GetObjectResponse?> GetObjectAsync(string key)
        {
            GetObjectRequest request = new GetObjectRequest {BucketName = this._bucketName, Key = key};
            AmazonS3Client amazonS3Client = await this.GetClientAsync().ConfigureAwait(false);
            try
            {
                return await amazonS3Client.GetObjectAsync(request).ConfigureAwait(false);
            }
            catch (AmazonS3Exception ex)
            {
                if (IsDoesntExist(ex))
                    return null;
                TryHandleException(ex);
                throw;
            }
        }

        private static void TryHandleException(AmazonS3Exception ex)
        {
            if (IsDoesntExist(ex))
                throw new StorageException(ErrorCode.NotFound, ex);
        }

        private static bool IsDoesntExist(AmazonS3Exception ex)
        {
            return ex.ErrorCode == "NoSuchKey";
        }

        public void Dispose()
        {
        }

        public Task<ITransaction> OpenTransactionAsync()
        {
            return Task.FromResult(EmptyTransaction.Instance);
        }
    }
}