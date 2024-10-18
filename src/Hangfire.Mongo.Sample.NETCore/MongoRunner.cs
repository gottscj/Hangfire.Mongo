using System;
using System.Threading.Tasks;
using Testcontainers.MongoDb;

namespace Hangfire.Mongo.Sample.NETCore
{
    public class MongoTestRunner : IAsyncDisposable
    {
        public readonly MongoDbContainer MongoDbContainer =
            new MongoDbBuilder()
                .WithImage("mongo:7.0")
                .Build();

        public string MongoConnectionString { get; private set; }

        public async Task Start()
        {
            await MongoDbContainer.StartAsync();
            MongoConnectionString = MongoDbContainer.GetConnectionString();
        }

        public async ValueTask DisposeAsync()
        {
            if (MongoDbContainer != null) await MongoDbContainer.DisposeAsync();
        }
    }
}