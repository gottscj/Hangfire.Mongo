using System;
using System.Reflection;
using System.Threading;
using MongoDB.Bson;
using MongoDB.Driver;
using Xunit.Sdk;

namespace Hangfire.Mongo.Tests.Utils
{
#pragma warning disable 1591
    public class CleanDatabaseAttribute : BeforeAfterTestAttribute
    {
        private static readonly object GlobalLock = new object();

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
            using (var context = ConnectionUtils.CreateConnection())
            {
                try
                {
                    context.Init(new MongoStorageOptions());

                    context.DistributedLock.DeleteMany(new BsonDocument());
                    context.StateData.DeleteMany(new BsonDocument());
                    context.Job.DeleteMany(new BsonDocument());
                    context.JobQueue.DeleteMany(new BsonDocument());
                    context.Server.DeleteMany(new BsonDocument());

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