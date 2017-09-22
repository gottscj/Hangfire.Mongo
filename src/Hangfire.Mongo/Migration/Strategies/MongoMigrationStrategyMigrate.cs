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

        public MongoMigrationStrategyMigrate(HangfireDbContext dbContext, MongoStorageOptions storageOptions)
            : base(dbContext, storageOptions)
        {
        }


        public override void Execute(MongoSchema fromSchema, MongoSchema toSchema)
        {
            if (fromSchema < MongoSchema.Version04)
            {
                var assemblyName = GetType().GetTypeInfo().Assembly.GetName();
                throw new InvalidOperationException(
                    $"{Environment.NewLine}{assemblyName.Name} version: {assemblyName.Version}, does not support migration from schema versions prior to {MongoSchema.Version04}." +
                    $"{Environment.NewLine}Please resolve this manually (e.g. by droping the database)." +
                    $"{Environment.NewLine}Please see https://github.com/sergeyzwezdin/Hangfire.Mongo#migration for further information.");
            }

            base.Execute(fromSchema, toSchema);
        }

    }
}
