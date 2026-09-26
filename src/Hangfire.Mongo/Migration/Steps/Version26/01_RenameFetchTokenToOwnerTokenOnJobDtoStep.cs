using Hangfire.Mongo.Dto;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Hangfire.Mongo.Migration.Steps.Version26
{
    /// <summary>
    /// Renames the schema 25 FetchToken field to OwnerToken on JobDto documents.
    /// </summary>
    internal class RenameFetchTokenToOwnerTokenOnJobDtoStep : IMongoMigrationStep
    {
        public MongoSchema TargetSchema => MongoSchema.Version26;
        public long Sequence => 1;

        public bool Execute(IMongoDatabase database, MongoStorageOptions storageOptions, IMongoMigrationContext migrationContext)
        {
            var jobGraph = database.GetCollection<BsonDocument>(storageOptions.Prefix + ".jobGraph");
            var jobFilter = Builders<BsonDocument>.Filter.Eq(
                "_t", new BsonArray { "BaseJobDto", "ExpiringJobDto", nameof(JobDto) });
            var hasFetchTokenWithoutOwnerToken = Builders<BsonDocument>.Filter.And(
                jobFilter,
                Builders<BsonDocument>.Filter.Exists("FetchToken"),
                Builders<BsonDocument>.Filter.Not(Builders<BsonDocument>.Filter.Exists("OwnerToken")));

            jobGraph.UpdateMany(
                hasFetchTokenWithoutOwnerToken,
                Builders<BsonDocument>.Update.Rename("FetchToken", "OwnerToken"));

            jobGraph.UpdateMany(
                Builders<BsonDocument>.Filter.And(jobFilter, Builders<BsonDocument>.Filter.Exists("FetchToken")),
                Builders<BsonDocument>.Update.Unset("FetchToken"));

            jobGraph.UpdateMany(
                Builders<BsonDocument>.Filter.And(
                    jobFilter,
                    Builders<BsonDocument>.Filter.Not(Builders<BsonDocument>.Filter.Exists("OwnerToken"))),
                Builders<BsonDocument>.Update.Set("OwnerToken", BsonNull.Value));

            return true;
        }
    }
}