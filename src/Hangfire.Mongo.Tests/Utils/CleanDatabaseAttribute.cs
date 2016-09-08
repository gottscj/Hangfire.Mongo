using System;
using System.Reflection;
using System.Threading;
using Hangfire.Mongo.Database;
using MongoDB.Bson;
using MongoDB.Driver;
using Xunit.Sdk;

namespace Hangfire.Mongo.Tests.Utils
{
#pragma warning disable 1591
    public class CleanDatabaseAttribute : BeforeAfterTestAttribute
    {
        private static readonly object GlobalLock = new object();

        public CleanDatabaseAttribute()
        {
        }

        public override void Before(MethodInfo methodUnderTest)
        {
            Monitor.Enter(GlobalLock);

            RecreateDatabaseAndInstallObjects();
        }

        public override void After(MethodInfo methodUnderTest)
        {
            Monitor.Exit(GlobalLock);
        }

        private static void RecreateDatabaseAndInstallObjects()
        {
            using (HangfireDbContext context = new HangfireDbContext(ConnectionUtils.GetConnectionString(), ConnectionUtils.GetDatabaseName()))
            {
                try
                {
                    context.Init();

                    context.Identifiers.DeleteMany(new BsonDocument());
                    context.DistributedLock.DeleteMany(new BsonDocument());
                    context.AggregatedCounter.DeleteMany(new BsonDocument());
                    context.Counter.DeleteMany(new BsonDocument());
                    context.Hash.DeleteMany(new BsonDocument());
                    context.Job.DeleteMany(new BsonDocument());
                    context.JobParameter.DeleteMany(new BsonDocument());
                    context.JobQueue.DeleteMany(new BsonDocument());
                    context.List.DeleteMany(new BsonDocument());
                    context.Server.DeleteMany(new BsonDocument());
                    context.Set.DeleteMany(new BsonDocument());
                    context.State.DeleteMany(new BsonDocument());

                }
                catch (MongoException ex)
                {
                    throw new InvalidOperationException("Unable to cleanup database.", ex);
                }
            }
        }
    }
#pragma warning restore 1591
}