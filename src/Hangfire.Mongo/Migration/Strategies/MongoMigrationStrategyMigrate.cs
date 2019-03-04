using System;
using System.Reflection;
using Hangfire.Mongo.Database;
using MongoDB.Driver;

namespace Hangfire.Mongo.Migration.Strategies
{
    /// <summary>
    /// Implements the "Migrate" strategy.
    /// Migrate the hangfire collections from current schema to required
    /// </summary>
    internal class MongoMigrationStrategyMigrate : MongoMigrationStrategyBase
    {

        public MongoMigrationStrategyMigrate(HangfireDbContext dbContext, MongoStorageOptions storageOptions, MongoMigrationRunner migrationRunner)
            : base(dbContext, storageOptions, migrationRunner)
        {
        }
    }
}
