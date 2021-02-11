using Amazon.S3;
using Amazon.S3.Model;
using Storage.Net.Blobs;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Itage.Storage.Selectel.S3
{
    internal static class Converter
    {
        /// <summary>
        /// AWS prepends all the user metadata with this prefix, and all of your own keys are prepended with this automatically
        /// </summary>
        private const string MetaDataHeaderPrefix = "x-amz-meta-";

        public static async Task UpdateMetadataAsync(
            AmazonS3Client client,
            Blob blob,
            string bucketName,
            string key)
        {
            CopyObjectRequest request = new CopyObjectRequest
            {
                SourceBucket = bucketName,
                DestinationBucket = bucketName,
                SourceKey = key,
                DestinationKey = key,
                MetadataDirective = S3MetadataDirective.REPLACE
            };
            foreach (KeyValuePair<string, string> keyValuePair in blob.Metadata)
                request.Metadata[keyValuePair.Key] = keyValuePair.Value;
            await client.CopyObjectAsync(request);
        }

        private static async Task AppendMetadataAsync(
            AmazonS3Client client,
            string bucketName,
            Blob blob,
            CancellationToken cancellationToken)
        {
            AddMetadata(blob, (await client.GetObjectMetadataAsync(bucketName, blob.FullPath, cancellationToken).ConfigureAwait(false)).Metadata);
        }

        public static async Task AppendMetadataAsync(
            AmazonS3Client client,
            string bucketName,
            IEnumerable<Blob> blobs,
            CancellationToken cancellationToken)
        {
            await Task.WhenAll(blobs.Select(blob => AppendMetadataAsync(client, bucketName, blob, cancellationToken))).ConfigureAwait(false);
        }

        public static Blob? ToBlob(this GetObjectMetadataResponse? obj, string fullPath)
        {
            if (obj == null)
                return null;
            Blob blob = new Blob(fullPath)
            {
                MD5 = obj.ETag.Trim('"'),
                Size = obj.ContentLength,
                LastModificationTime = obj.LastModified.ToUniversalTime()
            };
            AddMetadata(blob, obj.Metadata);
            blob.Properties["ETag"] = obj.ETag;
            return blob;
        }

        private static void AddMetadata(Blob blob, MetadataCollection metadata)
        {
            foreach (string key in metadata.Keys)
            {
                string str = metadata[key];
                string index = key;
                if (index.StartsWith(MetaDataHeaderPrefix))
                    index = index.Substring(MetaDataHeaderPrefix.Length);
                blob.Metadata[index] = str;
            }
        }

        private static Blob ToBlob(this S3Object s3Obj)
        {
            Blob blob = s3Obj.Key.EndsWith("/") ? new Blob(s3Obj.Key, BlobItemKind.Folder) : new Blob(s3Obj.Key);
            blob.Size = s3Obj.Size;
            blob.MD5 = s3Obj.ETag.Trim('"');
            blob.LastModificationTime = s3Obj.LastModified.ToUniversalTime();
            blob.Properties["StorageClass"] = s3Obj.StorageClass;
            blob.Properties["ETag"] = s3Obj.ETag;
            return blob;
        }

        public static IReadOnlyCollection<Blob> ToBlobs(
            this ListObjectsV2Response response,
            ListOptions options)
        {
            List<Blob> blobList = new List<Blob>();
            blobList.AddRange(response.S3Objects.Where(b => !b.Key.EndsWith("/")).Select(b => b.ToBlob()).Where(options.IsMatch).Where(b => options.BrowseFilter == null || options.BrowseFilter(b)));
            blobList.AddRange(response.CommonPrefixes.Select(p => new Blob(p, BlobItemKind.Folder)));
            return blobList;
        }
    }
}