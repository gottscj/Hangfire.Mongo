using System.Collections.Generic;
using System.Linq;
using Hangfire.Common;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Hangfire.Mongo.Migration.Steps.Version06
{
    /// <summary>
    /// Migrate Job collection
    /// </summary>
    internal class JobMigration : IMongoMigrationStep
    {
        public MongoSchema TargetSchema => MongoSchema.Version6;

        public long Sequence => 1;

        private Dictionary<int, string> JobIdMapping;

        public bool Execute(IMongoDatabase database, MongoStorageOptions storageOptions)
        {
            MigrateJob(database, storageOptions);
            MigrateJobQueue(database, storageOptions);
            MigrateScheduledJobs(database, storageOptions);
            return true;
        }


        private void MigrateJob(IMongoDatabase database, MongoStorageOptions storageOptions)
        {
            var jobCollection = database.GetCollection<BsonDocument>($@"{storageOptions.Prefix}.job");
            var jobParametersCollection = database.GetCollection<BsonDocument>($@"{storageOptions.Prefix}.jobParameter");
            var stateCollection = database.GetCollection<BsonDocument>($@"{storageOptions.Prefix}.state");

            var jobs = jobCollection.Find(_ => true).ToList();

            JobIdMapping = jobs
                .Select(j => j["_id"].AsInt32)
                .Distinct()
                .ToDictionary(jid => jid, jid => BsonObjectId.GenerateNewId().ToString());

            var migratedJobs = jobs.Select(job =>
            {
                var _id = job["_id"].AsInt32;
                var jobParameters = jobParametersCollection.Find(jp => jp["JobId"] == _id)
                    .ToList();
                var jobStates = stateCollection.Find(s => s["JobId"] == _id)
                    .SortBy(s => s["CreatedAt"])
                    .ToList();
                job["_id"] = JobIdMapping[_id];
                job["Parameters"] = new BsonDocument(jobParameters.ToDictionary(jp => jp["Name"].AsString, jp => jp["Value"].AsString));
                job["StateHistory"] = new BsonArray(jobStates.Select(s =>
                {
                    s.Remove("_id");
                    s.Remove("JobId");
                    s["Data"] = new BsonDocument(JobHelper.FromJson<Dictionary<string, string>>(s["Data"].AsString));
                    return s;
                }));
                job.Remove("StateId");
                return job;
            }).ToList();
            jobCollection.InsertMany(migratedJobs);

            var filter = Builders<BsonDocument>.Filter.In("_id", JobIdMapping.Keys);
            jobCollection.DeleteMany(filter);
        }


        private void MigrateScheduledJobs(IMongoDatabase database, MongoStorageOptions storageOptions)
        {
            var setCollection = database.GetCollection<BsonDocument>($@"{storageOptions.Prefix}.set");
            var migratedSetList = setCollection.Find(s => s["Key"] == "schedule").ToList().Select(s =>
            {
                s["Value"] = JobIdMapping[int.Parse(s["Value"].AsString)];
                s["_t"] = new BsonArray(new[] { "KeyValueDto", "ExpiringKeyValueDto", "SetDto" });
                return s;
            }).ToList();

            if (migratedSetList.Any())
            {
                var stateDataCollection = database.GetCollection<BsonDocument>($@"{storageOptions.Prefix}.statedata");
                stateDataCollection.InsertMany(migratedSetList);
            }
        }


        private void MigrateJobQueue(IMongoDatabase database, MongoStorageOptions storageOptions)
        {
            // Update jobQueue to reflect new job id
            var jobQueueCollection = database.GetCollection<BsonDocument>($@"{storageOptions.Prefix}.jobQueue");
            var migratedJobQueueList = jobQueueCollection.Find(_ => true).ToList().Select(jq =>
            {
                jq["_id"] = JobIdMapping[jq["JobId"].AsInt32];
                jq.Remove("JobId");
                return jq;
            }).ToList();

            if (migratedJobQueueList.Any())
            {
                jobQueueCollection.InsertMany(migratedJobQueueList);
            }

            var filter = Builders<BsonDocument>.Filter.In("_id", JobIdMapping.Keys);
            jobQueueCollection.DeleteMany(filter);
        }

    }
}
