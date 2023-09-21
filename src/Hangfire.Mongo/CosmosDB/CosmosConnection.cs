using System;
using Hangfire.Mongo.Database;

namespace Hangfire.Mongo.CosmosDB;

/// <inheritdoc />
public class CosmosConnection : MongoConnection
{
    /// <inheritdoc />
    public CosmosConnection(HangfireDbContext database, MongoStorageOptions storageOptions) 
        : base(database, storageOptions)
    {
    }

    /// <inheritdoc />
    public override DateTime GetUtcDateTime()
    {
        return DateTime.UtcNow;
    }
}