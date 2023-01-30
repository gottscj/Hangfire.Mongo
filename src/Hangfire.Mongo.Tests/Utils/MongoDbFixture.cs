using System;
using EphemeralMongo;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.Migration.Strategies;
using Hangfire.Mongo.Migration.Strategies.Backup;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.Linq;
using Xunit.Abstractions;
using Xunit.Sdk;

namespace Hangfire.Mongo.Tests.Utils;

public sealed class MongoDbFixture : IDisposable
{
    private const string DefaultDatabaseName = @"Hangfire-Mongo-Tests";

    private readonly IMongoRunner _runner;

    public MongoDbFixture(IMessageSink sink)
    {
        var options = new MongoRunnerOptions
        {
            StandardOuputLogger = text => sink.OnMessage(new DiagnosticMessage(text)),
            StandardErrorLogger = text => sink.OnMessage(new DiagnosticMessage($"MongoDB ERROR: {text}")),
        };
        _runner = MongoRunner.Run(options);
    }

    public void Dispose()
    {
        _runner.Dispose();
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
        var settings = MongoClientSettings.FromConnectionString(_runner.ConnectionString);
        return new MongoClient(settings);
    }
}