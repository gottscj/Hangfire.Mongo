using Hangfire.Mongo.Database;

namespace Hangfire.Mongo.DocumentDB;

///  <inheritdoc />
public class DocumentDbConnection : MongoConnection
{
    /// <inheritdoc />
    public DocumentDbConnection(HangfireDbContext database, DocumentDbStorageOptions storageOptions)
        : base(database, storageOptions)
    {
    }
}
