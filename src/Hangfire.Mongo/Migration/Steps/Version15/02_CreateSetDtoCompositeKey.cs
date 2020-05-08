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
        
        public bool Execute(IMongoDatabase database, MongoStorageOptions storageOptions, IMongoMigrationContext migrationContext)
        {
            var jobGraph = database.GetCollection<BsonDocument>(storageOptions.Prefix + ".jobGraph"); 

            CreateCompositeKey(jobGraph, typeof(SetDto));

            if (storageOptions is CosmosStorageOptions)
            {
                //Create combinate index for GetFirstByLowestScoreFromSet
                IndexKeysDefinitionBuilder<BsonDocument> cosmosIndexBuilder = Builders<BsonDocument>.IndexKeys;
                var id = cosmosIndexBuilder.Ascending("_id");
                var key = cosmosIndexBuilder.Ascending("Key");
                var score = cosmosIndexBuilder.Ascending("Score");
                var value = cosmosIndexBuilder.Ascending("Value");
                var exp = cosmosIndexBuilder.Ascending("ExpireAt");
                var indexModel = new CreateIndexModel<BsonDocument>(cosmosIndexBuilder.Combine(new[] { id, key, score, value, exp }),
                    new CreateIndexOptions
                    {
                        Name = "Idx_GetFirstByLowestScoreFromSet"
                    });
                jobGraph.Indexes.CreateOne(indexModel);
            }

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