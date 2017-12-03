using System;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.Migration;
using Hangfire.Server;
using MongoDB.Bson;
using MongoDB.Bson.IO;
using MongoDB.Driver;
using Xunit;

namespace Hangfire.Mongo.Tests
{
#pragma warning disable 1591
    [Collection("Database")]
    public class MongoDatabaseFiller
    {
        private static readonly EventWaitHandle RecurringEvent = new ManualResetEvent(false);
        private static readonly EventWaitHandle ScheduleEvent = new ManualResetEvent(false);
        private static readonly EventWaitHandle EnqueueEvent = new ManualResetEvent(false);
        private static readonly EventWaitHandle ContinueWithEvent = new ManualResetEvent(false);

        [Fact]
        public void Clean_Database_Filled()
        {
            var connectionString = "mongodb://localhost";
            var databaseName = "Mongo-Hangfire-Filled";

            // Make sure we start from scratch
            using (HangfireDbContext context = new HangfireDbContext(connectionString, databaseName))
            {
                context.Database.Client.DropDatabase(databaseName);
            }

            var storageOptions = new MongoStorageOptions
            {
                //MigrationOptions = new MongoMigrationOptions
                //{
                //    Strategy = MongoMigrationStrategy.Migrate,
                //    BackupStrategy = MongoBackupStrategy.Collections
                //}
            };
            var serverOptions = new BackgroundJobServerOptions
            {
                ShutdownTimeout = TimeSpan.FromMinutes(1)
            };

            JobStorage.Current = new MongoStorage(connectionString, databaseName, storageOptions);

            using (new BackgroundJobServer(serverOptions))
            {
                // Recurring Job
                RecurringJob.AddOrUpdate(() => ExecuteRecurringJob("Recurring job"), Cron.Minutely);

                // Scheduled job
                BackgroundJob.Schedule(() => ExecuteScheduledJob("Scheduled job"), TimeSpan.FromSeconds(30));

                // Enqueued job
                BackgroundJob.Enqueue(() => ExecuteEnqueuedJob("Enqueued job"));

                // Continued job
                var parentId = BackgroundJob.Schedule(() => ExecuteContinueWithJob("ContinueWith job", false), TimeSpan.FromSeconds(15));
                BackgroundJob.ContinueWith(parentId, () => ExecuteContinueWithJob("ContinueWith job continued", true));

                // Now the waiting game starts
                ScheduleEvent.WaitOne();
                BackgroundJob.Schedule(() => ExecuteScheduledJob("Scheduled job (*)"), TimeSpan.FromMinutes(30));

                ContinueWithEvent.WaitOne();
                RecurringEvent.WaitOne();

                EnqueueEvent.WaitOne();
                BackgroundJob.Enqueue(() => ExecuteEnqueuedJob("Enqueued job (*)"));
            }


            // Some data are cleaned up when hangfire shuts down.
            // Grab a copy so we can write it back - needed for migration tests.
            var connection = JobStorage.Current.GetConnection();
            connection.AnnounceServer("test-server", new ServerContext
            {
                WorkerCount = serverOptions.WorkerCount,
                Queues = serverOptions.Queues
            });

            connection.AcquireDistributedLock("test-lock", TimeSpan.FromSeconds(30));


            // Create database snapshot in zip file
            var schemaVersion = (int)MongoMigrationManager.RequiredSchemaVersion;
            using (var stream = new FileStream($@"Hangfire-Mongo-Schema-{schemaVersion}.zip", FileMode.Create))
            {
                BackupDatabaseToStream(connectionString, databaseName, stream);
            }
        }


        public void BackupDatabaseToStream(string connectionString, string databaseName, Stream stream)
        {
            using (var archive = new ZipArchive(stream, ZipArchiveMode.Create, true))
            {
                using (HangfireDbContext context = new HangfireDbContext(connectionString, databaseName))
                {
                    foreach (var collectionName in context.Database.ListCollections().ToList().Select(c => c["name"].AsString))
                    {
                        var fileName = $@"{collectionName}.json";
                        var collectionFile = archive.CreateEntry(fileName);

                        var collection = context.Database.GetCollection<BsonDocument>(collectionName);
                        var jsonDocs = collection.Find(Builders<BsonDocument>.Filter.Empty)
                            .ToList()
                            .Select(d => d.ToJson(JsonWriterSettings.Defaults));

                        using (var entryStream = collectionFile.Open())
                        {
                            using (var streamWriter = new StreamWriter(entryStream))
                            {
                                streamWriter.Write("[" + string.Join(",", jsonDocs) + "]");
                            }
                        }
                    }
                }
            }
        }


        public static void ExecuteRecurringJob(string argument)
        {
            Console.WriteLine(argument);
            RecurringEvent.Set();
        }


        public static void ExecuteScheduledJob(string argument)
        {
            Console.WriteLine(argument);
            ScheduleEvent.Set();
        }


        public static void ExecuteEnqueuedJob(string argument)
        {
            Console.WriteLine(argument);
            EnqueueEvent.Set();
        }


        public static void ExecuteContinueWithJob(string argument, bool continued)
        {
            Console.WriteLine(argument);
            if (continued)
            {
                ContinueWithEvent.Set();
            }
        }

    }

}