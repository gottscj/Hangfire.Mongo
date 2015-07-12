using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
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

                    List<Task> tasks = new List<Task>();

                    tasks.Add(context.Identifiers.DeleteManyAsync(new BsonDocument()));
                    tasks.Add(context.DistributedLock.DeleteManyAsync(new BsonDocument()));
                    tasks.Add(context.AggregatedCounter.DeleteManyAsync(new BsonDocument()));
                    tasks.Add(context.Counter.DeleteManyAsync(new BsonDocument()));
                    tasks.Add(context.Hash.DeleteManyAsync(new BsonDocument()));
                    tasks.Add(context.Job.DeleteManyAsync(new BsonDocument()));
                    tasks.Add(context.JobParameter.DeleteManyAsync(new BsonDocument()));
                    tasks.Add(context.JobQueue.DeleteManyAsync(new BsonDocument()));
                    tasks.Add(context.List.DeleteManyAsync(new BsonDocument()));
                    tasks.Add(context.Server.DeleteManyAsync(new BsonDocument()));
                    tasks.Add(context.Set.DeleteManyAsync(new BsonDocument()));
                    tasks.Add(context.State.DeleteManyAsync(new BsonDocument()));

                    Task.WaitAll(tasks.ToArray());
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