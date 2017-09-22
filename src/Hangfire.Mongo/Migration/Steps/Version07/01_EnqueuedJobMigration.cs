using System.Collections.Generic;
using System.Linq;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Hangfire.Mongo.Migration.Steps.Version07
{
    /// <summary>
    /// Migrate enqueued jobs
    /// </summary>
    internal class EnqueuedJobMigration : IMongoMigrationStep
    {
        public MongoSchema TargetSchema => MongoSchema.Version07;

        public long Sequence => 1;

        public bool Execute(IMongoDatabase database, MongoStorageOptions storageOptions, IMongoMigrationBag migrationBag)
        {
            // Update jobQueue to reflect new job id
            var jobQueueCollection = database.GetCollection<BsonDocument>($@"{storageOptions.Prefix}.jobQueue");
            var filterBuilder = Builders<BsonDocument>.Filter;
            var filter = filterBuilder.Not(filterBuilder.Exists("JobId"));
            var migratedJobQueueList = jobQueueCollection.Find(filter).ToList().Select(jq =>
            {
                jq["JobId"] = jq["_id"];
                jq["_id"] = new BsonObjectId(ObjectId.GenerateNewId());
                return jq;
            }).ToList();

            if (migratedJobQueueList.Any())
            {
                jobQueueCollection.InsertMany(migratedJobQueueList);
            }

            return true;
        }

    }
}
