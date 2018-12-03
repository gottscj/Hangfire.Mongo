using System;
using System.Threading;
using Castle.Components.DictionaryAdapter;
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
    public class ConventionPackFacts
    {
        [Fact]
        public void CamelCaseConvention_HangfireMongoDtos_StillInPascal()
        {
            // ARRANGE
            var mongoStorage = new MongoStorage(
                ConnectionUtils.GetConnectionString(),
                ConnectionUtils.GetDatabaseName());

            JobStorage.Current = mongoStorage;
            
            var jobScheduled = false;
            
            var conventionPack = new ConventionPack {new CamelCaseElementNameConvention()}; 
            ConventionRegistry.Register("CamelCase", conventionPack, t => true);
            
            // ACT
            using (new BackgroundJobServer(new BackgroundJobServerOptions{SchedulePollingInterval = TimeSpan.FromMilliseconds(100)}))
            {
                BackgroundJob.Enqueue(() => Signal.Set());
                jobScheduled = Signal.WaitOne(TimeSpan.FromSeconds(1));
            }
            
            // ASSERT
            var jobGraphCollectionName = mongoStorage.Connection.JobGraph.CollectionNamespace.CollectionName;
            var jobDto = mongoStorage
                .Connection
                .Database
                .GetCollection<BsonDocument>(jobGraphCollectionName)
                .Find(new BsonDocument("expireAt", new BsonDocument("$exists", true)))
                .FirstOrDefault();
            
            Assert.Null(jobDto);
            Assert.True(jobScheduled, "Expected job to be scheduled");
        }

    }

    public class Signal
    {
        private static readonly AutoResetEvent AutoResetEvent = new AutoResetEvent(false);

        public static void Set()
        {
            AutoResetEvent.Set();
        }

        public static bool WaitOne(TimeSpan timeSpan)
        {
            return AutoResetEvent.WaitOne(timeSpan);
        }
    }
}