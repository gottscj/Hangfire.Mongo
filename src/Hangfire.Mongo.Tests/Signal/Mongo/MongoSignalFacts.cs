using System;
using System.Threading;
using System.Threading.Tasks;
using Hangfire.Mongo.Signal.Mongo;
using Hangfire.Mongo.Tests.Utils;
using MongoDB.Driver;
using Xunit;

namespace Hangfire.Mongo.Tests.Signal.Mongo
{
    [Collection("Signal")]
    public class MongoSignalFacts
    {

        [Fact]
        public void Ctor_ThrowsAnException_WhenConnectionIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(() => new MongoSignal(null));

            Assert.Equal("signalCollection", exception.ParamName);
        }

        [Fact]
        public void Signal_ThrowsAnException_WhenNameIsNull()
        {
            ConnectionUtils.UseConnection(connection =>
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
            ConnectionUtils.UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentException>(() =>
                {
                    var signal = new MongoSignal(connection.Signal);
                    signal.Set(string.Empty);
                });

                Assert.Equal("name", exception.ParamName);
            });
        }

        [Fact]
        public void Signal_ShouleStore_Always()
        {
            ConnectionUtils.UseConnection(connection =>
            {
                // make signal name unique to void conflict with other unit tests
                var name = Guid.NewGuid().ToString();
                var signal = new MongoSignal(connection.Signal);
                signal.Set(name);

                var signals = connection
                    .Signal.Count(s => s.Name == name);

                Assert.Equal(1, signals);
            });
        }

        [Fact]
        public void Wait_ThrowsAnException_WhenNameIsNull()
        {
            ConnectionUtils.UseConnection(connection =>
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
            ConnectionUtils.UseConnection(connection =>
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
            ConnectionUtils.UseConnection(connection =>
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
            ConnectionUtils.UseConnection(connection =>
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
            ConnectionUtils.UseConnection(connection =>
            {
                var exception = Assert.Throws<ArgumentException>(() =>
                {
                    var signal = new MongoSignal(connection.Signal);
                    signal.Wait(new[] { "test", "" }, default(CancellationToken));
                });

                Assert.Equal("names", exception.ParamName);
            });
        }

    }
}
