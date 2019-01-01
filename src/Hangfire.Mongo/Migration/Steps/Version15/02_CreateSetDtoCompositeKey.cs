using System;
using Hangfire.Mongo.Dto;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Hangfire.Mongo.Migration.Steps.Version15
{
    internal class CreateCompositeKeys : IMongoMigrationStep
    {
        public MongoSchema TargetSchema => MongoSchema.Version15;
        
        public long Sequence => 2;
        
        public bool Execute(IMongoDatabase database, MongoStorageOptions storageOptions, IMongoMigrationBag migrationBag)
        {
            var jobGraph = database.GetCollection<BsonDocument>(storageOptions.Prefix + ".jobGraph"); 

            CreateCompositeKey(jobGraph, typeof(SetDto));

            return true;
        }

        private static void CreateCompositeKey(IMongoCollection<BsonDocument> jobGraph, Type type)
        {
            var documents = jobGraph.FindSync(new BsonDocument("_t", type.Name))
                .ToEnumerable();

            foreach (var document in documents)
            {
                document["Key"] = $"{document["Key"].AsString}:{document["Value"].AsString}";
                jobGraph.ReplaceOne(new BsonDocument("_id", document["_id"]), document);
            }
        }
    }
}