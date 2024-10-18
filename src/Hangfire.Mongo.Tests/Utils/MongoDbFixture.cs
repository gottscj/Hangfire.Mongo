using System;
using System.Threading.Tasks;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.Migration.Strategies;
using Hangfire.Mongo.Migration.Strategies.Backup;
using MongoDB.Bson;
using MongoDB.Driver;
using Testcontainers.MongoDb;
using Xunit;

namespace Hangfire.Mongo.Tests.Utils;

public sealed class MongoIntegrationTestFixture : IAsyncLifetime
{
    private const string DefaultDatabaseName = @"Hangfire-Mongo-Tests";

    public readonly MongoDbContainer MongoDbContainer =
        new MongoDbBuilder()
            .WithImage("mongo:7.0")
            .Build();

    public string MongoConnectionString { get; private set; }

    public async Task InitializeAsync()
    {
        await MongoDbContainer.StartAsync();

        MongoConnectionString = MongoDbContainer.GetConnectionString();
    }

    public async Task DisposeAsync()
    {
        await MongoDbContainer.DisposeAsync();
    }
    public MongoStorage CreateStorage(string databaseName = null)
    {
        var storageOptions = new MongoStorageOptions
        {
            MigrationOptions = new MongoMigrationOptions
            {
                MigrationStrategy = new DropMongoMigrationStrategy(),
                BackupStrategy = new NoneMongoBackupStrategy()
            }
        };
        return CreateStorage(storageOptions, databaseName);
    }

    public MongoStorage CreateStorage(MongoStorageOptions storageOptions, string databaseName=null)
    {
        var client = GetMongoClient();
        return new MongoStorage(client, databaseName ?? DefaultDatabaseName, storageOptions);
    }

    public HangfireDbContext CreateDbContext(string dbName = null)
    {
        var client = GetMongoClient();
        return new HangfireDbContext(client, dbName ?? DefaultDatabaseName);
    }

    public void CleanDatabase(string dbName = null)
    {
        try
        {
            var context = CreateDbContext(dbName);
            context.DistributedLock.DeleteMany(new BsonDocument());
            context.JobGraph.DeleteMany(new BsonDocument());
            context.Server.DeleteMany(new BsonDocument());
            context.Database.DropCollection(context.Notifications.CollectionNamespace.CollectionName);
        }
        catch (MongoException ex)
        {
            throw new InvalidOperationException("Unable to cleanup database.", ex);
        }
    }

    private MongoClient GetMongoClient()
    {
        var settings = MongoClientSettings.FromConnectionString(MongoConnectionString);
        return new MongoClient(settings);
    }
}