using System;
using MongoDB.Bson;
using MongoDB.Driver;
using Newtonsoft.Json.Linq;

namespace Hangfire.Mongo.Migration.Steps.Version15
{
    internal class MakeServerDataEmbeddedDocument : IMongoMigrationStep
    {
        public MongoSchema TargetSchema => MongoSchema.Version15;

        public long Sequence => 1;
        
        public bool Execute(IMongoDatabase database, MongoStorageOptions storageOptions, IMongoMigrationBag migrationBag)
        {
            var serverCollection = database.GetCollection<BsonDocument>(storageOptions.Prefix + ".server");
            var servers = serverCollection.Find(new BsonDocument()).ToList();
            
            foreach (var server in servers)
            {
                if (!server.Contains("Data"))
                {
                    continue;
                }
                
                var jsonData = JObject.Parse(server["Data"].AsString);
                var update = new BsonDocument
                {
                    ["$set"] = new BsonDocument
                    {
                        ["WorkerCount"] = int.Parse(jsonData["WorkerCount"].Value<string>()),
                        ["Queues"] = new BsonArray(jsonData["Queues"].ToObject<string[]>()),
                        ["StartedAt"] = jsonData["StartedAt"]?.ToObject<DateTime?>(),
                        ["LastHeartbeat"] = server["LastHeartbeat"]
                    },
                    ["$unset"] = new BsonDocument("Data", "")  
                };

                serverCollection.UpdateOne(new BsonDocument("_id", server["_id"]), update);
            }
            
            return true;
        }
    }
}