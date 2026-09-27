using System;
using Hangfire.Mongo.Dto;
using Hangfire.Mongo.Migration;
using Hangfire.Mongo.Migration.Steps.Version26;
using Hangfire.Mongo.Tests.Utils;
using MongoDB.Bson;
using MongoDB.Driver;
using Xunit;

namespace Hangfire.Mongo.Tests.Migration
{
    [Collection("Database")]
    public class Version26MigrationStepFacts
    {
        private readonly IMongoDatabase _database;
        private readonly MongoStorageOptions _storageOptions;

        public Version26MigrationStepFacts(MongoIntegrationTestFixture fixture)
        {
            var dbContext = fixture.CreateDbContext();
            _database = dbContext.Database;
            _storageOptions = new MongoStorageOptions();
        }

        [Fact]
        public void ExecuteStep00_AddOwnerTokenFields_Success()
        {
            var migration = new AddOwnerTokenFieldToLockDtosStep();
            var locks = _database.GetCollection<BsonDocument>(_storageOptions.Prefix + ".locks");
            var migrationLocks = _database.GetCollection<BsonDocument>(_storageOptions.Prefix + ".migrationLock");
            locks.DeleteMany("{}");
            migrationLocks.DeleteMany("{}");

            var distributedLock = new BsonDocument
            {
                ["_id"] = ObjectId.GenerateNewId(),
                [nameof(DistributedLockDto.Resource)] = "resource",
                [nameof(DistributedLockDto.ExpireAt)] = DateTime.UtcNow.AddMinutes(1)
            };
            var migrationLock = new BsonDocument
            {
                ["_id"] = MigrationLock.LockId,
                [nameof(MigrationLockDto.ExpireAt)] = DateTime.UtcNow.AddMinutes(1)
            };
            locks.InsertOne(distributedLock);
            migrationLocks.InsertOne(migrationLock);

            var result = migration.Execute(_database, _storageOptions, new MongoMigrationContext());

            Assert.True(result);
            Assert.Equal(BsonNull.Value, locks.Find(new BsonDocument("_id", distributedLock["_id"])).Single()[nameof(DistributedLockDto.OwnerToken)]);
            Assert.Equal(BsonNull.Value, migrationLocks.Find(new BsonDocument("_id", migrationLock["_id"])).Single()[nameof(MigrationLockDto.OwnerToken)]);
        }

        [Fact]
        public void ExecuteStep00_AddOwnerTokenFields_IsIdempotent()
        {
            var migration = new AddOwnerTokenFieldToLockDtosStep();
            var locks = _database.GetCollection<BsonDocument>(_storageOptions.Prefix + ".locks");
            var migrationLocks = _database.GetCollection<BsonDocument>(_storageOptions.Prefix + ".migrationLock");
            locks.DeleteMany("{}");
            migrationLocks.DeleteMany("{}");
            var distributedOwner = Guid.NewGuid().ToString("N");
            var migrationOwner = Guid.NewGuid().ToString("N");

            locks.InsertOne(new BsonDocument
            {
                ["_id"] = ObjectId.GenerateNewId(),
                [nameof(DistributedLockDto.Resource)] = "resource",
                [nameof(DistributedLockDto.OwnerToken)] = distributedOwner,
                [nameof(DistributedLockDto.ExpireAt)] = DateTime.UtcNow.AddMinutes(1)
            });
            migrationLocks.InsertOne(new BsonDocument
            {
                ["_id"] = MigrationLock.LockId,
                [nameof(MigrationLockDto.OwnerToken)] = migrationOwner,
                [nameof(MigrationLockDto.ExpireAt)] = DateTime.UtcNow.AddMinutes(1)
            });

            migration.Execute(_database, _storageOptions, new MongoMigrationContext());
            migration.Execute(_database, _storageOptions, new MongoMigrationContext());

            Assert.Equal(distributedOwner, locks.Find(new BsonDocument()).Single()[nameof(DistributedLockDto.OwnerToken)].AsString);
            Assert.Equal(migrationOwner, migrationLocks.Find(new BsonDocument()).Single()[nameof(MigrationLockDto.OwnerToken)].AsString);
        }

        [Fact]
        public void ExecuteStep01_RenameFetchTokenToOwnerToken_Success()
        {
            var migration = new RenameFetchTokenToOwnerTokenOnJobDtoStep();
            var jobGraph = _database.GetCollection<BsonDocument>(_storageOptions.Prefix + ".jobGraph");
            jobGraph.DeleteMany("{}");
            var token = Guid.NewGuid().ToString("N");
            var job = new JobDto { OwnerToken = token }.Serialize();
            job["FetchToken"] = job[nameof(JobDto.OwnerToken)];
            job.Remove(nameof(JobDto.OwnerToken));
            jobGraph.InsertOne(job);

            var result = migration.Execute(_database, _storageOptions, new MongoMigrationContext());

            Assert.True(result);
            var migratedJob = jobGraph.Find(new BsonDocument("_id", job["_id"])).Single();
            Assert.Equal(token, migratedJob[nameof(JobDto.OwnerToken)].AsString);
            Assert.False(migratedJob.Contains("FetchToken"));
        }

        [Fact]
        public void ExecuteStep01_RenameFetchTokenToOwnerToken_IsIdempotent()
        {
            var migration = new RenameFetchTokenToOwnerTokenOnJobDtoStep();
            var jobGraph = _database.GetCollection<BsonDocument>(_storageOptions.Prefix + ".jobGraph");
            jobGraph.DeleteMany("{}");
            var ownerToken = Guid.NewGuid().ToString("N");
            var job = new JobDto { OwnerToken = ownerToken }.Serialize();
            job["FetchToken"] = Guid.NewGuid().ToString("N");
            jobGraph.InsertOne(job);

            migration.Execute(_database, _storageOptions, new MongoMigrationContext());
            migration.Execute(_database, _storageOptions, new MongoMigrationContext());

            var migratedJob = jobGraph.Find(new BsonDocument("_id", job["_id"])).Single();
            Assert.Equal(ownerToken, migratedJob[nameof(JobDto.OwnerToken)].AsString);
            Assert.False(migratedJob.Contains("FetchToken"));
        }
    }
}