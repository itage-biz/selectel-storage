using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using FluentFTP;
using Polly;
using Polly.Retry;
using Storage.Net.Blobs;


// ReSharper disable once CheckNamespace
namespace Storage.Net
{
    internal class PassiveFtpStorage : IBlobStorage
    {
        private static readonly AsyncRetryPolicy RetryPolicy = Policy.Handle<FtpException>().RetryAsync(3);
        private readonly FtpClient _client;
        private readonly bool _dispose;
        private readonly string _prefix;

        public PassiveFtpStorage(NetworkCredential credential, string prefix = "", bool dispose = false)
        {
            _client = new FtpClient(credential.Domain, credential) {DataConnectionType = FtpDataConnectionType.PASV};
            _client.Capabilities.Add(FtpCapability.MDTM);
            _prefix = prefix;
            _dispose = dispose;
        }

        private async Task<FtpClient> GetClientAsync()
        {
            if (!this._client.IsConnected)
                await this._client.ConnectAsync().ConfigureAwait(false);
            return this._client;
        }

        public async Task<IReadOnlyCollection<Blob>> ListAsync(
            ListOptions? options = null,
            CancellationToken cancellationToken = default)
        {
            FtpClient ftpClient = await this.GetClientAsync().ConfigureAwait(false);
            options ??= new ListOptions();
            string folderPath = ToAbsolute(options.FolderPath);
            CancellationToken cancellationToken1 = new CancellationToken();
            FtpListItem[] ftpListItemArray =
                await ftpClient.GetListingAsync(folderPath, cancellationToken1).ConfigureAwait(false);

            List<Blob> blobList = new List<Blob>();

            foreach (FtpListItem ff in ftpListItemArray)
            {
                if (options.FilePrefix != null && !ff.Name.StartsWith(options.FilePrefix))
                {
                    continue;
                }

                Blob? blobId = ToBlobId(ff);
                if (blobId == null || (options.BrowseFilter != null && !options.BrowseFilter(blobId)))
                {
                    continue;
                }

                blobList.Add(blobId);
                if (!options.MaxResults.HasValue)
                {
                    continue;
                }

                int count = blobList.Count;
                int num = options.MaxResults.Value;
                if (count >= num)
                    break;

            }

            return blobList;
        }

        private string ToAbsolute(string optionsFolderPath)
        {
            return optionsFolderPath.StartsWith("/")
                ? $"/{_prefix}{optionsFolderPath}"
                : $"{_prefix}/{optionsFolderPath}";
        }

        private Blob? ToBlobId(FtpListItem ff)
        {
            if (ff.Type != FtpFileSystemObjectType.Directory && ff.Type != FtpFileSystemObjectType.File)
                return null;
            Blob blob = new Blob(
                FromAbsolute(ff.FullName),
                ff.Type == FtpFileSystemObjectType.File ? BlobItemKind.File : BlobItemKind.Folder);
            if (ff.RawPermissions != null)
                blob.Properties["RawPermissions"] = ff.RawPermissions;
            blob.LastModificationTime = ff.Modified;
            blob.Size = ff.Size;
            return blob;
        }

        private string FromAbsolute(string fullName)
        {
            return fullName.StartsWith($"/{_prefix}/")
                ? fullName.Substring(_prefix.Length + 1)
                : fullName;
        }

        public async Task DeleteAsync(
            IEnumerable<string> fullPaths,
            CancellationToken cancellationToken = default)
        {
            FtpClient client = await this.GetClientAsync().ConfigureAwait(false);
            foreach (string fullPath in fullPaths)
            {
                string path = ToAbsolute(fullPath);
                try
                {
                    await client.DeleteFileAsync(path, cancellationToken).ConfigureAwait(false);
                }
                catch (FtpCommandException ex) when (ex.CompletionCode == "550")
                {
                    await client.DeleteDirectoryAsync(path, cancellationToken).ConfigureAwait(false);
                }
            }
        }

        public async Task<IReadOnlyCollection<bool>> ExistsAsync(
            IEnumerable<string> ids,
            CancellationToken cancellationToken = default)
        {
            FtpClient client = await this.GetClientAsync().ConfigureAwait(false);
            List<bool> results = new List<bool>();
            foreach (string id in ids)
            {
                results.Add(await client.FileExistsAsync(ToAbsolute(id), cancellationToken).ConfigureAwait(false));
            }

            return results;
        }

        public async Task<IReadOnlyCollection<Blob>> GetBlobsAsync(
            IEnumerable<string> ids,
            CancellationToken cancellationToken = default)
        {
            FtpClient client = await this.GetClientAsync().ConfigureAwait(false);
            List<Blob> results = new List<Blob>();
            foreach (string id in ids)
            {
                string path = ToAbsolute(id);
                string cPath = StoragePath.Normalize(path, true);
                FtpListItem[] files = await client
                    .GetListingAsync(StoragePath.GetParent(cPath), cancellationToken)
                    .ConfigureAwait(false);

                FtpListItem? ftpListItem = files.FirstOrDefault(i => i.FullName == cPath);
                if (ftpListItem == null)
                {
                    results.Add(null!);
                }
                else
                {
                    Blob blob = new Blob(path) {Size = ftpListItem.Size, LastModificationTime = ftpListItem.Modified};
                    results.Add(blob);
                }
            }

            return results;
        }

        public Task SetBlobsAsync(IEnumerable<Blob> blobs,
            CancellationToken cancellationToken = default)
        {
            throw new NotSupportedException();
        }

        public async Task<Stream> OpenReadAsync(
            string fullPath,
            CancellationToken cancellationToken = default)
        {
            FtpClient ftpClient = await this.GetClientAsync().ConfigureAwait(false);
            try
            {
                return await ftpClient
                    .OpenReadAsync(ToAbsolute(fullPath), FtpDataType.Binary, 0L, true, cancellationToken)
                    .ConfigureAwait(false);
            }
            catch (FtpCommandException ex) when (ex.CompletionCode == "550")
            {
                return null!;
            }
        }

        public Task<ITransaction> OpenTransactionAsync()
        {
            return Task.FromResult(EmptyTransaction.Instance);
        }

        public async Task WriteAsync(
            string fullPath,
            Stream dataStream,
            bool append = false,
            CancellationToken cancellationToken = default)
        {
            FtpClient client = await this.GetClientAsync().ConfigureAwait(false);
            await RetryPolicy.ExecuteAsync(async () =>
            {
                using Stream dest = await client
                    .OpenWriteAsync(ToAbsolute(fullPath), FtpDataType.Binary, true, cancellationToken)
                    .ConfigureAwait(false);
                await dataStream.CopyToAsync(dest, 1024 * 32, cancellationToken).ConfigureAwait(false);
            }).ConfigureAwait(false);
        }

        public void Dispose()
        {
            if (!this._dispose || !this._client.IsDisposed)
                return;
            this._client.Dispose();
        }
    }
}