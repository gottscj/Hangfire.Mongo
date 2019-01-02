using System.Collections.Generic;
using System.Linq;
using Hangfire.Mongo.Dto;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Hangfire.Mongo.Migration.Steps.Version16
{
    internal class UpdateSetDtoKeyAndValueField : IMongoMigrationStep
    {
        public MongoSchema TargetSchema { get; } = MongoSchema.Version16;
        public long Sequence { get; } = 0;
        
        public bool Execute(IMongoDatabase database, MongoStorageOptions storageOptions, IMongoMigrationBag migrationBag)
        {
            var jobGraph = database.GetCollection<BsonDocument>(storageOptions.Prefix + ".jobGraph"); 

            var documents = jobGraph.FindSync(new BsonDocument("_t", nameof(SetDto)))
                .ToEnumerable();

            var updates = new List<UpdateOneModel<BsonDocument>>();
            foreach (var document in documents)
            {
                var compositeKey = document["Key"].AsString;
                var splitIndex = compositeKey.IndexOf(':');
                string value;
                string key;
                if (splitIndex < 0)
                {
                    key = compositeKey;
                    value = document.Contains("Value") ? document["Value"].AsString : string.Empty; 
                }
                else
                {
                    key = compositeKey.Substring(0, splitIndex);
                    value = compositeKey.Substring(splitIndex + 1);
                }
                
                var filter = new BsonDocument("_id", document["_id"]);
                var update = new BsonDocument("$set", new BsonDocument
                {
                    ["Value"] = value,
                    ["Key"] = $"{key}<{value}>"
                });
                
                updates.Add(new UpdateOneModel<BsonDocument>(filter, update));
            }

            if (!updates.Any())
            {
                return true;
            }
            

            jobGraph.BulkWrite(updates);
            
            return true;
        }
    }
}