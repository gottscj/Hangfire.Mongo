using MongoDB.Driver;
#pragma warning disable 1591

namespace Hangfire.Mongo.Migration.Strategies
{
    /// <summary>
    /// Implements the "Migrate" strategy.
    /// Migrate the hangfire collections from current schema to required
    /// </summary>
    public class MigrateMongoMigrationStrategy : MongoMigrationStrategy
    {
        public MigrateMongoMigrationStrategy() 
            : this(new MongoMigrationContext())
        {
        }

        public MigrateMongoMigrationStrategy(IMongoMigrationContext mongoMigrationContext) 
            : base(mongoMigrationContext)
        {
        }
        
        protected override void ExecuteStrategy(IMongoDatabase database, MongoSchema fromSchema, MongoSchema toSchema)
        {
            // nothing to do
        }
    }
}
