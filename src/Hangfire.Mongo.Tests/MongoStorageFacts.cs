using System;
using System.Linq;
using Hangfire.Mongo.Tests.Utils;
using Hangfire.Storage;
using MongoDB.Driver;
using Xunit;

namespace Hangfire.Mongo.Tests
{
#pragma warning disable 1591
    [Collection("Database")]
    public class MongoStorageFacts
    {
        private readonly MongoStorage _storage;

        public MongoStorageFacts(MongoIntegrationTestFixture fixture)
        {
            fixture.CleanDatabase();
            _storage = fixture.CreateStorage();
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenConnectionStringIsEmpty()
        {
            var exception = Assert.Throws<MongoConfigurationException>(() => new MongoStorage(MongoClientSettings.FromConnectionString(""), "database"));

            Assert.NotNull(exception);
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenDatabaseNameIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(() => new MongoStorage(MongoClientSettings.FromConnectionString("mongodb://localhost"), null));

            Assert.Equal("databaseName", exception.ParamName);
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenStorageOptionsValueIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(() => new MongoStorage(MongoClientSettings.FromConnectionString("mongodb://localhost"), "database", null));

            Assert.Equal("storageOptions", exception.ParamName);
        }
        
        [Fact]
        public void Ctor_DoesNotSupportCappedAndTailNotificationChosen_ThrowsAnException()
        {
            var exception = Assert.Throws<NotSupportedException>(() => new MongoStorage(
                MongoClientSettings.FromConnectionString("mongodb://localhost"), 
                "test",
                new MongoStorageOptions
                {
                    CheckQueuedJobsStrategy = CheckQueuedJobsStrategy.TailNotificationsCollection,
                    SupportsCappedCollection = false
                }));

            Assert.Contains("CheckQueuedJobsStrategy, cannot be TailNotificationsCollection if", exception.Message);
        }

        [Fact]
        public void GetMonitoringApi_ReturnsNonNullInstance()
        {
            IMonitoringApi api = _storage.GetMonitoringApi();
            Assert.NotNull(api);
        }

        [Fact]
        public void GetConnection_ReturnsNonNullInstance()
        {
            using (IStorageConnection connection = _storage.GetConnection())
            {
                Assert.NotNull(connection);
            }
        }

        [Fact]
        public void GetComponents_ReturnsAllNeededComponents()
        {
            var components = _storage.GetComponents();

            Type[] componentTypes = components.Select(x => x.GetType()).ToArray();
            Assert.Contains(typeof(MongoExpirationManager), componentTypes);
        }

    }
#pragma warning restore 1591
}