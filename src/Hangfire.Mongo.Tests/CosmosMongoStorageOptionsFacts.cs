using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace Hangfire.Mongo.Tests
{
#pragma warning disable 1591
    [Collection("Database")]
    public class CosmosMongoStorageOptionsFacts
    {
        [Fact]
        public void Ctor_SetsTheDefaultOptions()
        {
            CosmosMongoStorageOptions storageOptions = new CosmosMongoStorageOptions();

            Assert.Equal("hangfire", storageOptions.Prefix);
            Assert.Null(storageOptions.InvisibilityTimeout);
        }

        [Fact]
        public void Ctor_SetsTheDefaultOptions_ShouldGenerateClientId()
        {
            var storageOptions = new CosmosMongoStorageOptions();
            Assert.False(String.IsNullOrWhiteSpace(storageOptions.ClientId));
        }

        [Fact]
        public void Ctor_SetsTheDefaultOptions_ShouldGenerateUniqueClientId()
        {
            var storageOptions1 = new CosmosMongoStorageOptions();
            var storageOptions2 = new CosmosMongoStorageOptions();
            var storageOptions3 = new CosmosMongoStorageOptions();

            IEnumerable<string> result = new[] { storageOptions1.ClientId, storageOptions2.ClientId, storageOptions3.ClientId }.Distinct();

            Assert.Equal(3, result.Count());
        }

        [Fact]
        public void Set_CosmosHourlyTtl_SetsTheValue()
        {
            var storageOptions = new CosmosMongoStorageOptions(10);
            Assert.Equal(10, storageOptions.CosmosHourlyTtl);
        }
    }
#pragma warning restore 1591
}