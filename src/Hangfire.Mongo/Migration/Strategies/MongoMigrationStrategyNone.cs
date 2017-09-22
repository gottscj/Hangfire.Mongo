using System;
using System.Reflection;

namespace Hangfire.Mongo.Migration.Strategies
{
    /// <summary>
    /// Implements the "None" strategy.
    /// Not much to execute just throw an exception.
    /// We do not want to continue on an obsolete schema.
    /// </summary>
    internal class MongoMigrationStrategyNone : IMongoMigrationStrategy
    {
        public MongoMigrationStrategyNone()
        {
        }

        public void Execute(MongoSchema fromSchema, MongoSchema toSchema)
        {
            var assemblyName = GetType().GetTypeInfo().Assembly.GetName();
            throw new InvalidOperationException(
                $"{Environment.NewLine}{assemblyName.Name} version: {assemblyName.Version}, introduces a new schema version that requires migration." +
                $"{Environment.NewLine}You can choose a migration strategy by setting the {nameof(MongoStorageOptions)}.{nameof(MongoStorageOptions.MigrationOptions)} property." +
                $"{Environment.NewLine}Please see https://github.com/sergeyzwezdin/Hangfire.Mongo#migration for further information.");
        }
    }
}
