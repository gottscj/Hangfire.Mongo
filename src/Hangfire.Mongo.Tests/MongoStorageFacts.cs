﻿using System;
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

        [Fact, CleanDatabase]
        public void GetMonitoringApi_ReturnsNonNullInstance()
        {
            MongoStorage storage = ConnectionUtils.CreateStorage();
            IMonitoringApi api = storage.GetMonitoringApi();
            Assert.NotNull(api);
        }

        [Fact, CleanDatabase]
        public void GetConnection_ReturnsNonNullInstance()
        {
            MongoStorage storage = ConnectionUtils.CreateStorage();
            using (IStorageConnection connection = storage.GetConnection())
            {
                Assert.NotNull(connection);
            }
        }

        [Fact]
        public void GetComponents_ReturnsAllNeededComponents()
        {
            MongoStorage storage = ConnectionUtils.CreateStorage();

            var components = storage.GetComponents();

            Type[] componentTypes = components.Select(x => x.GetType()).ToArray();
            Assert.Contains(typeof(MongoExpirationManager), componentTypes);
        }

    }
#pragma warning restore 1591
}