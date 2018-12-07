using System;
using System.Threading;
using Hangfire.Mongo.Tests.Utils;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Conventions;
using MongoDB.Driver;
using Xunit;

namespace Hangfire.Mongo.Tests
{

    [Collection("Database")]
    public class MongoConventionsFacts
    {
        public class Signal
        {
            private static readonly AutoResetEvent ResetEvent = new AutoResetEvent(false);

            public static void Set()
            {
                ResetEvent.Set();
            }

            public static bool WaitOne(TimeSpan timeSpan)
            {
                return ResetEvent.WaitOne(timeSpan);
            }
        }
        
        [Fact, CleanDatabase(false)]
        public void Conventions_UsesOwnConventionsForDtoNameSpace_WhenCamelCaseIsRegistered()
        {
            // ARRANGE
            var conventionPack = new ConventionPack { new CamelCaseElementNameConvention() };
            ConventionRegistry.Register("camelCase", conventionPack, t => true);

            // ACT
            // This line will throw during migration if camelCase is active for the Dto namespace.
            var connection = ConnectionUtils.CreateDbContext();

            // ASSERT
            Assert.NotNull(connection);
        }

        [Fact]
        public void CamelCaseConvention_HangfireMongoDtos_StillInPascal()
        {
            // ARRANGE
            var mongoStorage = new MongoStorage(
                ConnectionUtils.GetConnectionString(),
                ConnectionUtils.GetDatabaseName());

            var dbContext = ConnectionUtils.CreateDbContext();
            JobStorage.Current = mongoStorage;
            
            bool jobScheduled;
            
            var conventionPack = new ConventionPack {new CamelCaseElementNameConvention()}; 
            ConventionRegistry.Register("CamelCase", conventionPack, t => true);
            
            // ACT
            using (new BackgroundJobServer(new BackgroundJobServerOptions{SchedulePollingInterval = TimeSpan.FromMilliseconds(100)}))
            {
                BackgroundJob.Enqueue(() => Signal.Set());
                jobScheduled = Signal.WaitOne(TimeSpan.FromSeconds(1));
            }
            
            // ASSERT
            var jobGraphCollectionName = dbContext.JobGraph.CollectionNamespace.CollectionName;
            var jobDto = dbContext
                .Database
                .GetCollection<BsonDocument>(jobGraphCollectionName)
                .Find(new BsonDocument("expireAt", new BsonDocument("$exists", true)))
                .FirstOrDefault();
            
            Assert.Null(jobDto);
            Assert.True(jobScheduled, "Expected job to be scheduled");
        }
        
    }

}
