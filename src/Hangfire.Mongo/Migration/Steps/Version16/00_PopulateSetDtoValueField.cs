using System.Linq;
using Hangfire.Mongo.Dto;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Hangfire.Mongo.Migration.Steps.Version16
{
    internal class PopulateSetDtoValueField : IMongoMigrationStep
    {
        public MongoSchema TargetSchema { get; } = MongoSchema.Version16;
        public long Sequence { get; } = 0;
        
        public bool Execute(IMongoDatabase database, MongoStorageOptions storageOptions, IMongoMigrationBag migrationBag)
        {
            var jobGraph = database.GetCollection<BsonDocument>(storageOptions.Prefix + ".jobGraph"); 

            var documents = jobGraph.FindSync(new BsonDocument("_t", nameof(SetDto)))
                .ToEnumerable();

            var writeModels = (from document in documents 
                let compositeKey = document["Key"].AsString 
                let value = compositeKey.Substring(compositeKey.IndexOf(':') + 1) 
                let filter = new BsonDocument("_id", document["_id"]) 
                let update = new BsonDocument("$set", new BsonDocument("Value", value)) 
                select new UpdateOneModel<BsonDocument>(filter, update))
                .ToList();

            if (!writeModels.Any())
            {
                return true;
            }

            jobGraph.BulkWrite(writeModels);
            
            return true;
        }
    }
}