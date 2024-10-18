using System;
using System.Collections.Generic;
using Hangfire.Common;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.Dto;
using Hangfire.Mongo.Tests.Utils;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Conventions;
using MongoDB.Driver;
using Xunit;

namespace Hangfire.Mongo.Tests
{
    [Collection("Database")]
    public class MongoDiscriminatorTests
    {
        private readonly HangfireDbContext _dbContext;

        public MongoDiscriminatorTests(MongoIntegrationTestFixture fixture)
        {
            _dbContext = fixture.CreateDbContext();
        }

        [Fact]
        public void AddJob_FromWriteOnly_FetchOK()
        {
            // ARRANGE
            ConventionRegistry.Register(
               "Hangfire Mongo Conventions",
               new ConventionPack
            {
                 new DelegateClassMapConvention("Hangfire Mongo Convention", cm =>
                 cm.SetDiscriminator(cm.ClassType.FullName))
            },
               t => t.FullName.StartsWith("Hangfire.Mongo") && t.IsAssignableFrom(typeof(BaseJobDto)));
            var createdAt = new DateTime(2012, 12, 12, 0, 0, 0, 0, DateTimeKind.Utc);
            var job = Job.FromExpression(() => HangfireTestJobs.SampleMethod("Hello"), "queue");
            string jobId;
            using (var transaction = new MongoWriteOnlyTransaction(_dbContext, new MongoStorageOptions()))
            {
                jobId = transaction.CreateExpiredJob(job,
                new Dictionary<string, string> { { "Key1", "Value1" }, { "Key2", "Value2" } },
                createdAt,
                TimeSpan.FromDays(1));
                transaction.Commit();
            }

            // ACT
            var jobDoc = _dbContext.JobGraph.Find(new BsonDocument
            {
                ["_t"] = nameof(JobDto),
                ["_id"] = ObjectId.Parse(jobId)
            })
                .FirstOrDefault();


            // ASSERT
            Assert.Equal("queue", job.Queue);
            Assert.NotNull(jobDoc);
            var jobDto = new JobDto(jobDoc);
            // queue should always be null when first created. see #359
            Assert.Null(jobDto.Queue);
         }

    }
}