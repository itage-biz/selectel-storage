using System.Net;
using Amazon.S3;
using Itage.Storage.Selectel.S3;
using Storage.Net.Blobs;
using Storage.Net.ConnectionString;
using Storage.Net.Messaging;

// ReSharper disable once CheckNamespace
namespace Storage.Net
{
    /// <summary>Selectel Storage Factory</summary>
    public static class Factory
    {
        /// <summary>Adds selectel storage to Storage.Net registry</summary>
        /// <param name="factory"></param>
        /// <returns></returns>
        public static IModulesFactory UseSelectelStorage(this IModulesFactory factory)
        {
            return factory
                .Use(new SelectelFtpModule())
                .Use(new SelectelS3Module());
        }

        private class SelectelS3Module : IExternalModule
        {
            public IConnectionFactory ConnectionFactory => new SelectelS3ConnectionFactory();
        }

        private class SelectelFtpModule : IExternalModule
        {
            public IConnectionFactory ConnectionFactory => new SelectelFtpConnectionFactory();
        }

        private class SelectelFtpConnectionFactory : IConnectionFactory
        {
            private const string Prefix = "selectelftp";
            private const string SelectelFtpDomain = "ftp.selcdn.ru";

            public IBlobStorage? CreateBlobStorage(StorageConnectionString connectionString)
            {
                if (connectionString.Prefix != Prefix)
                    return null;
                connectionString.GetRequired("username", true, out string userName);
                connectionString.GetRequired("password", true, out string password);
                connectionString.GetRequired("bucket", true, out string bucket);
                return new PassiveFtpStorage(new NetworkCredential(userName, password, SelectelFtpDomain), bucket);
            }

            public IMessenger? CreateMessenger(StorageConnectionString connectionString)
            {
                return null;
            }
        }

        private class SelectelS3ConnectionFactory : IConnectionFactory
        {
            private const string Prefix = "selectel";
            private const string SelectelServiceUri = "https://s3.selcdn.ru";
            private const string AwsSignatureVersion = "AWS";

            public IBlobStorage? CreateBlobStorage(StorageConnectionString connectionString)
            {
                if (connectionString.Prefix != Prefix)
                    return null;
                connectionString.GetRequired("username", true, out string userName);
                connectionString.GetRequired("password", true, out string password);
                connectionString.GetRequired("bucket", true, out string bucket);
                var s3 = new SelectelAwsS3BlobStorage(userName, password, bucket,
                    new AmazonS3Config
                    {
                        ServiceURL = SelectelServiceUri,
                        ForcePathStyle = true,
                        SignatureVersion = AwsSignatureVersion
                    });
                return s3;
            }

            public IMessenger? CreateMessenger(StorageConnectionString connectionString)
            {
                return null;
            }
        }
    }
}