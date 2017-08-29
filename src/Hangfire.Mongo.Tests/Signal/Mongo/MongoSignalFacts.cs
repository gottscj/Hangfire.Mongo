using System;
using System.Threading;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.Signal.Mongo;
using Hangfire.Mongo.Tests.Utils;
using MongoDB.Driver;
using Xunit;

namespace Hangfire.Mongo.Tests.Signal.Mongo
{
    [Collection("Database")]
    public class MongoSignalFacts
    {

        [Fact]
        public void Ctor_ThrowsAnException_WhenConnectionIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(() => new MongoSignal(null));

            Assert.Equal("signal", exception.ParamName);
        }

        [Fact]
        public void Signal_ThrowsAnException_WhenNameIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(() =>
                {
                    var signal = new MongoSignal(connection.Signal);
                    signal.Set(null);
                });

                Assert.Equal("name", exception.ParamName);
            });
        }

        [Fact]
        public void Signal_ThrowsAnException_WhenNameIsEmpty()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentException>(() =>
                {
                    var signal = new MongoSignal(connection.Signal);
                    signal.Set(string.Empty);
                });

                Assert.Equal("name", exception.ParamName);
            });
        }

        [Fact, CleanDatabase]
        public void Signal_ShouleStore_Always()
        {
            UseConnection(connection =>
            {
                var signal = new MongoSignal(connection.Signal);
                signal.Set("thename");

                var signals = connection
                    .Signal.Find(s => s.Signaled == true && s.Name == "thename")
                        .ToList();

                Assert.Equal(1, signals.Count);
            });
        }

        [Fact]
        public void Wait_ThrowsAnException_WhenNameIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(() =>
                {
                    var signal = new MongoSignal(connection.Signal);
                    signal.Wait(null, TimeSpan.FromSeconds(5));
                });

                Assert.Equal("name", exception.ParamName);
            });
        }

        [Fact]
        public void Wait_ThrowsAnException_WhenNameIsEmpty()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentException>(() =>
                {
                    var signal = new MongoSignal(connection.Signal);
                    signal.Wait(string.Empty, TimeSpan.FromSeconds(5));
                });

                Assert.Equal("name", exception.ParamName);
            });
        }

        [Fact]
        public void Wait_ThrowsAnException_WhenNamesIsNull()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentNullException>(() =>
                {
                    var signal = new MongoSignal(connection.Signal);
                    signal.Wait((string[])null, default(CancellationToken));
                });

                Assert.Equal("names", exception.ParamName);
            });
        }

        [Fact]
        public void Wait_ThrowsAnException_WhenNamesIsZeroLength()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentException>(() =>
                {
                    var signal = new MongoSignal(connection.Signal);
                    signal.Wait(new string[0], default(CancellationToken));
                });

                Assert.Equal("names", exception.ParamName);
            });
        }

        [Fact]
        public void Wait_ThrowsAnException_WhenNamesContainsEmptyName()
        {
            UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentException>(() =>
                {
                    var signal = new MongoSignal(connection.Signal);
                    signal.Wait(new[] { "test", "" }, default(CancellationToken));
                });

                Assert.Equal("names", exception.ParamName);
            });
        }

        private static void UseConnection(Action<HangfireDbContext> action)
        {
            using (var connection = ConnectionUtils.CreateConnection())
            {
                action(connection);
            }
        }
    }
}
