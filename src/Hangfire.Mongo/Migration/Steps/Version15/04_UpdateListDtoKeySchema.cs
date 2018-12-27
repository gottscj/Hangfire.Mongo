using MongoDB.Bson;
using MongoDB.Driver;

namespace Hangfire.Mongo.Migration.Steps.Version15
{
    internal class UpdateListDtoKeySchema : IMongoMigrationStep
     {
         public MongoSchema TargetSchema => MongoSchema.Version15;
         public long Sequence => 4;

         public bool Execute(IMongoDatabase database, MongoStorageOptions storageOptions,
             IMongoMigrationBag migrationBag)
         {
             var jobGraph = database.GetCollection<BsonDocument>($@"{storageOptions.Prefix}.jobGraph");

             var filter = new BsonDocument("_t", "ListDto");
             var update = new BsonDocument
             {
                 ["$rename"] = new BsonDocument("Key", "Item"),
                 ["$set"] = new BsonDocument("_t", BsonArray.Create(new[] {"BaseJobDto", "ExpiringJobDto", "ListDto"}))
             };

             jobGraph.UpdateMany(filter, update);

             return true;
         }
     }
 }