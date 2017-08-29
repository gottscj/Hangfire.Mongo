using System;
using System.Linq;
using System.Reflection;
using System.Threading;
using Hangfire.Mongo.Migration;
using MongoDB.Bson;
using MongoDB.Driver;
using Xunit.Sdk;

namespace Hangfire.Mongo.Tests.Utils
{
    internal class CleanDatabaseAttribute : BeforeAfterTestAttribute
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
            try
            {
                var client = new MongoClient(ConnectionUtils.GetConnectionString());
                var database = client.GetDatabase(ConnectionUtils.GetDatabaseName());
                var storageOptions = new MongoStorageOptions();
                var names = MongoMigrationManager.RequiredSchemaVersion.CollectionNames(storageOptions.Prefix);
                foreach (var name in names.Where(n => !n.EndsWith("schema") && !n.EndsWith("signal")))
                {
                    var collection = database.GetCollection<BsonDocument>(name);
                    collection.DeleteMany(new BsonDocument());
                }
            }
            catch (MongoException ex)
            {
                throw new InvalidOperationException("Unable to cleanup database.", ex);
            }
        }
    }
}