using Hangfire.Mongo.Migration;
using MongoDB.Driver;

namespace Hangfire.Mongo.Tests.Migration.Mongo;

public class TestMongoMigrationManager : MongoMigrationManager
{
    public TestMongoMigrationManager(MongoStorageOptions storageOptions, IMongoDatabase database) 
        : base(storageOptions, database)
    {
    }

    public override MongoSchema RequiredSchemaVersion { get; } = MongoSchema.Version20;
}