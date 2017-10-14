using System;
using System.Linq;
using System.Reflection;
using System.Threading;
using Hangfire.Mongo.Dto;
using Hangfire.Mongo.Migration;
using MongoDB.Bson;
using MongoDB.Driver;
using Xunit.Sdk;

namespace Hangfire.Mongo.Tests.Utils
{
    internal class CleanDatabaseAttribute : BeforeAfterTestAttribute
    {
        private static readonly object GlobalLock = new object();

        public bool Initialized { get; set; }

        public CleanDatabaseAttribute() : this(true)
        {
        }

        public CleanDatabaseAttribute(bool initialized)
        {
            Initialized = initialized;
        }

        public override void Before(MethodInfo methodUnderTest)
        {
            Monitor.Enter(GlobalLock);

            if (Initialized)
            {
                RecreateDatabaseAndInstallObjects();
                return;
            }

            // Drop the database and do not run any
            // migrations to initialize the database.
            var client = new MongoClient(ConnectionUtils.GetConnectionString());
            client.DropDatabase(ConnectionUtils.GetDatabaseName());
        }

        public override void After(MethodInfo methodUnderTest)
        {
            Monitor.Exit(GlobalLock);
        }

        private static void RecreateDatabaseAndInstallObjects()
        {
            try
            {
                var client = new MongoClient(ConnectionUtils.GetConnectionString());
                var database = client.GetDatabase(ConnectionUtils.GetDatabaseName());
                var storageOptions = new MongoStorageOptions();
                var names = MongoMigrationManager.RequiredSchemaVersion.CollectionNames(storageOptions.Prefix);
                foreach (var name in names.Where(n => !n.EndsWith(".schema")))
                {
                    var collection = database.GetCollection<BsonDocument>(name);
                    if (name.EndsWith(".signal"))
                    {
                        CleanSignalCollection(collection);
                    }
                    else
                    {
                        CleanCollection(collection);
                    }
                }
            }
            catch (MongoException ex)
            {
                throw new InvalidOperationException("Unable to cleanup database.", ex);
            }
        }

        private static void CleanSignalCollection(IMongoCollection<BsonDocument> collection)
        {
            var update = Builders<BsonDocument>.Update.Set(nameof(SignalDto.Signaled), false);
            collection.UpdateMany(new BsonDocument(), update);
        }

        private static void CleanCollection(IMongoCollection<BsonDocument> collection)
        {
            collection.DeleteMany(new BsonDocument());
        }
    }
}