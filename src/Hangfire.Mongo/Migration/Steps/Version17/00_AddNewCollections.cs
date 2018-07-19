using MongoDB.Driver;

namespace Hangfire.Mongo.Migration.Steps.Version17
{
    internal class AddCappedEnqueuedJobsCollection : IMongoMigrationStep
    {
        public MongoSchema TargetSchema { get; } = MongoSchema.Version17;
        public long Sequence { get; } = 0;
        
        public bool Execute(IMongoDatabase database, MongoStorageOptions storageOptions, IMongoMigrationBag migrationBag)
        {
            database.CreateCollection(storageOptions.Prefix + ".enqueuedJobs", new CreateCollectionOptions
            {
                Capped = true,
                MaxSize = 4096
            });
            
            return true;
        }
    }
}