using System.Collections.Generic;
using System.Linq;
using Hangfire.Common;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Hangfire.Mongo.Migration.Steps.Version06
{
    /// <summary>
    /// Migrate jobs
    /// </summary>
    internal class JobMigration : IMongoMigrationStep
    {
        public MongoSchema TargetSchema => MongoSchema.Version6;

        public long Sequence => 1;

        public bool Execute(IMongoDatabase database, MongoStorageOptions storageOptions, IMongoMigrationBag migrationBag)
        {
            var jobCollection = database.GetCollection<BsonDocument>($@"{storageOptions.Prefix}.job");
            var jobParametersCollection = database.GetCollection<BsonDocument>($@"{storageOptions.Prefix}.jobParameter");
            var stateCollection = database.GetCollection<BsonDocument>($@"{storageOptions.Prefix}.state");

            var filter = Builders<BsonDocument>.Filter.Type("_id", BsonType.Int32);
            var jobs = jobCollection.Find(filter).ToList();

            var jobIdMapping = jobs
                .Select(j => j["_id"].AsInt32)
                .Distinct()
                .ToDictionary(jid => jid, jid => new BsonObjectId(ObjectId.GenerateNewId()).ToString());
            migrationBag.SetItem("JobIdMapping", jobIdMapping);

            var migratedJobs = jobs.Select(job =>
            {
                var id = job["_id"].AsInt32;
                var jobParameters = jobParametersCollection.Find(jp => jp["JobId"] == id)
                    .ToList();
                var jobStates = stateCollection.Find(s => s["JobId"] == id)
                    .SortBy(s => s["CreatedAt"])
                    .ToList();
                job["_id"] = jobIdMapping[id];
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

            if (migratedJobs.Any())
            {
                jobCollection.InsertMany(migratedJobs);
            }

            jobCollection.DeleteMany(filter);

            return true;
        }

    }
}
