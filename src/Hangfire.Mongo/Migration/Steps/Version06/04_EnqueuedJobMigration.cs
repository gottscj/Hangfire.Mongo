using System.Collections.Generic;
using System.Linq;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Hangfire.Mongo.Migration.Steps.Version06
{
    /// <summary>
    /// Migrate enqueued jobs
    /// </summary>
    internal class EnqueuedJobMigration : IMongoMigrationStep
    {
        public MongoSchema TargetSchema => MongoSchema.Version06;

        public long Sequence => 4;

        public bool Execute(IMongoDatabase database, MongoStorageOptions storageOptions, IMongoMigrationBag migrationBag)
        {
            var jobIdMapping = migrationBag.GetItem<Dictionary<int, string>>("JobIdMapping");

            // Update jobQueue to reflect new job id
            var jobQueueCollection = database.GetCollection<BsonDocument>($@"{storageOptions.Prefix}.jobQueue");
            var migratedJobQueueList = jobQueueCollection.Find(_ => true).ToList().Select(jq =>
            {
                // NOTE:
                // This is a "hack". We actually migrate to schema version 7,
                // where we should have migrated to schema version 6.
                // But there is a issue in schema version 6 that will
                // cause us to loose information.
                // Since any version of Hangfire.Mongo running schema version 6
                // does not have migration, it is save to do this.
                jq["_id"] = new BsonObjectId(ObjectId.GenerateNewId());
                jq["JobId"] = jobIdMapping[jq["JobId"].AsInt32];
                return jq;
            }).ToList();

            if (migratedJobQueueList.Any())
            {
                jobQueueCollection.InsertMany(migratedJobQueueList);
            }

            var filter = Builders<BsonDocument>.Filter.In("JobId", jobIdMapping.Keys);
            jobQueueCollection.DeleteMany(filter);

            return true;
        }

    }
}
