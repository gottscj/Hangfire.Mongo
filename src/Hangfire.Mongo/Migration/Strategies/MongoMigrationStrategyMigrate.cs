using System;
using System.Reflection;
using MongoDB.Driver;

namespace Hangfire.Mongo.Migration.Strategies
{
    /// <summary>
    /// Implements the "Migrate" strategy.
    /// Migrate the hangfire collections from current schema to required
    /// </summary>
    internal class MongoMigrationStrategyMigrate : MongoMigrationStrategyBase
    {

        public MongoMigrationStrategyMigrate(IMongoDatabase database, MongoStorageOptions storageOptions, MongoMigrationRunner migrationRunner)
            : base(database, storageOptions, migrationRunner)
        {
        }
    }
}
