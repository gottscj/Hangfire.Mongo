using MongoDB.Driver;

namespace Hangfire.Mongo.DocumentDB;

/// <summary>
/// Cosmos DB storage
/// </summary>
public class DocumentDbStorage : MongoStorage
{
    /// <summary>
    /// ctor
    /// </summary>
    /// <param name="mongoClient"></param>
    /// <param name="databaseName"></param>
    /// <param name="storageOptions"></param>
    public DocumentDbStorage(IMongoClient mongoClient, string databaseName, DocumentDbStorageOptions storageOptions)
        : base(mongoClient, databaseName, storageOptions)
    {
    }
}