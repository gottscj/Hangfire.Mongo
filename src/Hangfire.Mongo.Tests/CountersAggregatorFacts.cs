using System;
using System.Threading;
using Hangfire.Mongo.Dto;
using Hangfire.Mongo.Helpers;
using Hangfire.Mongo.Tests.Utils;
using MongoDB.Bson;
using Xunit;

namespace Hangfire.Mongo.Tests
{
#pragma warning disable 1591
    [Collection("Database")]
    public class CountersAggregatorFacts
    {
        [Fact, CleanDatabase]
        public void CountersAggregatorExecutesProperly()
        {
            var storage = new MongoStorage(ConnectionUtils.GetConnectionString(), ConnectionUtils.GetDatabaseName());
            using (var connection = (MongoConnection)storage.GetConnection())
            {
                using (var database = ConnectionUtils.CreateConnection())
                {
                    // Arrange
                    AsyncHelper.RunSync(() => database.Counter.InsertOneAsync(new CounterDto
                    {
                        Key = "key",
                        Value = 1,
                        ExpireAt = DateTime.UtcNow.AddHours(1)
                    }));

                    var aggregator = new CountersAggregator(storage, TimeSpan.Zero);
                    var cts = new CancellationTokenSource();
                    cts.Cancel();

                    // Act
                    aggregator.Execute(cts.Token);

                    // Assert
                    Assert.Equal(1, AsyncHelper.RunSync(() => database.AggregatedCounter.CountAsync(new BsonDocument())));
                }
            }
        }
    }
#pragma warning restore 1591
}