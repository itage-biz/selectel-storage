# selectel-storage

Storage.Net Plugin for Selectel

## Description

Plugin for [Storage.Net](https://github.com/aloneguid/storage) for Selectel object storage

Supports S3 and FTP protocols

## Usage

```csharp
StorageFactory.Modules.UseSelectelStorage();
IBlobStorage s3Storage = StorageFactory.Blobs.FromConnectionString("selectel://username=USER;password=PASSWORD;bucket=BUCKET");
IBlobStorage ftpStorageStorage = StorageFactory.Blobs.FromConnectionString("selectelftp://username=USER;password=PASSWORD;bucket=BUCKET");
```
