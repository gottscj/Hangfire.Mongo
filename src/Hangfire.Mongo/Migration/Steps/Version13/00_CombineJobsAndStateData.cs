using System.Collections.Generic;
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
            var jobGraphCollectionName = storageOptions.Prefix + ".jobGraph";
            
            var stateDataCopy = new Dictionary<string,object>
            {
                {"aggregate" , storageOptions.Prefix + ".stateData"},
                {"pipeline", new []
                    {
                        new Dictionary<string, object> { { "$match" , new BsonDocument() }},
                        new Dictionary<string, object> { { "$out" , jobGraphCollectionName}}
                    }
                }
            };
            
            var jobsCopy = new Dictionary<string,object>
            {
                {"aggregate" , storageOptions.Prefix + ".job"},
                {"pipeline", new []
                    {
                        new Dictionary<string, object> { { "$match" , new BsonDocument() }},
                        new Dictionary<string, object> { { "$out" , jobGraphCollectionName}}
                    }
                }
            };
            
            var stateDataCommand = new BsonDocumentCommand<BsonDocument>(new BsonDocument(stateDataCopy));
            var jobCommand = new BsonDocumentCommand<BsonDocument>(new BsonDocument(jobsCopy));
            database.RunCommand(stateDataCommand);
            database.RunCommand(jobCommand);
            
            var indexBuilder = Builders<BsonDocument>.IndexKeys;
            var jobGraph = database.GetCollection<BsonDocument>(jobGraphCollectionName);
            jobGraph.TryCreateIndexes(indexBuilder.Descending, "StateName", "ExpireAt", "_t");
            jobGraph.TryCreateIndexes(indexBuilder.Ascending, "Key");
            
            return true;
        }
    }
}