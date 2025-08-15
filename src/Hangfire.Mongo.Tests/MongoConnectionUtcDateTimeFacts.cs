using System;
using Hangfire.Logging;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.Tests.Utils;
using Hangfire.Mongo.UtcDateTime;
using Xunit;

namespace Hangfire.Mongo.Tests
{
    [Collection("Database")]
    public class MongoConnectionUtcDateTimeFacts
    {
        private readonly HangfireDbContext _dbContext;

        public MongoConnectionUtcDateTimeFacts(MongoIntegrationTestFixture fixture)
        {
            fixture.CleanDatabase();
            _dbContext = fixture.CreateDbContext();
        }

        [Fact]
        public void GetUtcDateTime_ReturnsValueFromConfiguredStrategy()
        {
            var expected = new DateTime(2020, 1, 2, 3, 4, 5, DateTimeKind.Utc);

            var storageOptions = new MongoStorageOptions
            {
                UtcDateTimeStrategy = new FixedUtcDateTimeStrategy(expected)
            };

            var connection = new MongoConnection(_dbContext, storageOptions);

            var now = connection.GetUtcDateTime();

            Assert.Equal(expected, now);
        }

        [Fact]
        public void GetUtcDateTime_FallsBackToAvailableStrategies_WhenConfiguredStrategyFails()
        {
            var fallback = new DateTime(2021, 6, 7, 8, 9, 10, DateTimeKind.Utc);

            var storageOptions = new MongoStorageOptions
            {
                UtcDateTimeStrategy = new ThrowingUtcDateTimeStrategy(),
                EnabledUtcDateTimeStrategies = [new FixedUtcDateTimeStrategy(fallback)]
            };

            var connection = new MongoConnection(_dbContext, storageOptions);

            var now = connection.GetUtcDateTime();

            Assert.Equal(fallback, now);
        }

        [Fact]
        public void GetUtcDateTime_ThrowsInvalidOperation_WhenAllStrategiesFail()
        {
            var storageOptions = new MongoStorageOptions
            {
                EnabledUtcDateTimeStrategies = [new ThrowingUtcDateTimeStrategy(), new ThrowingUtcDateTimeStrategy()]
            };

            var connection = new MongoConnection(_dbContext, storageOptions);

            Assert.Throws<InvalidOperationException>(() => connection.GetUtcDateTime());
        }

        private sealed class FixedUtcDateTimeStrategy : UtcDateTimeStrategy
        {
            private readonly DateTime _value;

            public FixedUtcDateTimeStrategy(DateTime value)
            {
                _value = value;
            }

            public override DateTime GetUtcDateTime(HangfireDbContext dbContext, ILog logger)
            {
                return _value;
            }
        }

        private sealed class ThrowingUtcDateTimeStrategy : UtcDateTimeStrategy
        {
            public override DateTime GetUtcDateTime(HangfireDbContext dbContext, ILog logger)
            {
                throw new InvalidOperationException("strategy failed");
            }
        }
    }
}
