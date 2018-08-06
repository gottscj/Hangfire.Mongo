using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Hangfire.Mongo.Dto;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Hangfire.Mongo.Migration.Steps.Version13
{
    internal class CombineJobsAndStateData : IMongoMigrationStep
    {
        public MongoSchema TargetSchema => MongoSchema.Version13;

        public long Sequence => 0;
        
        public bool Execute(IMongoDatabase database, MongoStorageOptions storageOptions, IMongoMigrationBag migrationBag)
        {
            var stateDataFindTask = database
                .GetCollection<BsonDocument>(storageOptions.Prefix + ".stateData")
                .Find(new BsonDocument())
                .ToListAsync();

            var jobFindTask = database
                .GetCollection<BsonDocument>(storageOptions.Prefix + ".job")
                .Find(new BsonDocument())
                .ToListAsync();

            // run in parallel, make sure we dont deadlock if we have a synchronization context
            Task.Run(() => Task.WhenAll(stateDataFindTask, jobFindTask)).GetAwaiter().GetResult();

            var jobs = jobFindTask.Result;
            var stateData = stateDataFindTask.Result;
            
            foreach (var data in stateData)
            {
                var typeName = "";
                if (data.TryGetValue("_t", out var typeValue))
                {
                    typeName = typeValue is BsonArray ? data["_t"].AsBsonArray.Last().AsString : data["_t"].AsString;
                }
                else
                {
                    throw new InvalidOperationException($"Expected '_t' element in stateData entity, got: {data.ToJson()}");
                }
                
                data["_t"] = new BsonArray(new []{nameof(BaseJobDto), nameof(KeyJobDto), typeName});
            }
            
            foreach (var job in jobs)
            {
                job["_t"] = new BsonArray(new[] {nameof(BaseJobDto), nameof(JobDto)});
            }

            var jobGraphEntities = jobs.Concat(stateData);
            if(jobGraphEntities.Any())
            {
                database
                .GetCollection<BsonDocument>(storageOptions.Prefix + ".jobGraph")
                .InsertMany(jobGraphEntities);
            }
            
            return true;
        }
    }
}