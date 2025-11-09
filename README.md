Hangfire.Mongo
===============

[![Build](https://github.com/Hangfire-Mongo/Hangfire.Mongo/actions/workflows/build.yml/badge.svg)](https://github.com/Hangfire-Mongo/Hangfire.Mongo/actions/workflows/build.yml) [![NuGet downloads](https://img.shields.io/nuget/dt/Hangfire.Mongo.svg)](https://www.nuget.org/packages/Hangfire.Mongo) [![License](https://img.shields.io/badge/license-MIT-blue.svg)](https://raw.githubusercontent.com/sergun/Hangfire.Mongo/master/LICENSE)

A MongoDB storage provider for Hangfire. Use MongoDB-compatible servers (including Azure Cosmos DB configured with the MongoDB API, and AWS DocumentDB) to persist and process Hangfire jobs.

### Why Hangfire.Mongo?
- Reliable job storage and state management in MongoDB.
- Multiple queue notification strategies (change streams, tailable collections, polling).
- Schema migration and backup strategies with configurable behavior.
- Extension points to customize collection creation, serialization and UTC handling.
- Works with ASP.NET Core and console-hosted Hangfire servers.

### Prerequisites
- .NET Standard / .NET Core compatible runtime used by your application.
- MongoDB server (community, Atlas, or other compatible servers). For Cosmos DB use the MongoDB API endpoint.

Hangfire (project and docs)
- Website: https://www.hangfire.io/
- Documentation: https://docs.hangfire.io/en/latest/

### Installation

Install from NuGet (recommended):

```bash
dotnet add package Hangfire.Mongo
```

Or via the Package Manager Console in Visual Studio:

```
PM> Install-Package Hangfire.Mongo
```

### Quick start — ASP.NET Core

Add Hangfire and Hangfire.Mongo in your Startup/Program configuration:

```csharp
// Program.cs or Startup.cs
var mongoUrl = new MongoUrl("mongodb://localhost:27017/jobs");
var mongoClient = new MongoClient(mongoUrl.ToMongoUrl());

services.AddHangfire(configuration => configuration
    .SetDataCompatibilityLevel(CompatibilityLevel.Version_180)
    .UseSimpleAssemblyNameTypeSerializer()
    .UseRecommendedSerializerSettings()
    .UseMongoStorage(mongoClient, mongoUrl.DatabaseName, new MongoStorageOptions
    {
        Prefix = "hangfire.mongo",
        CheckConnection = true,
        MigrationOptions = new MongoMigrationOptions
        {
            MigrationStrategy = new MigrateMongoMigrationStrategy(),
            BackupStrategy = new CollectionMongoBackupStrategy()
        }
    }));

services.AddHangfireServer();
```

### Quick start — Console

```csharp
var options = new MongoStorageOptions
{
    MigrationOptions = new MongoMigrationOptions
    {
        MigrationStrategy = new DropMongoMigrationStrategy(),
        BackupStrategy = new NoneMongoBackupStrategy()
    }
};

using var storage = new MongoStorage(
    MongoClientSettings.FromConnectionString("mongodb://localhost:27017"),
    "jobs",
    options);

using var server = new BackgroundJobServer(storage);
```

Configuration highlights
- Prefix: prefix for Hangfire collection names (default: no prefix).
- CheckConnection: verify connectivity at startup (recommended for production).
- InvisibilityTimeout: controls how long a job remains in Processing before becoming visible again; configure to avoid stuck jobs.
- CheckQueuedJobsStrategy: choose between Watch (change streams), Poll, or TailNotificationsCollection.

### Cosmos DB (MongoDB API) — Getting started

Hangfire.Mongo works with Azure Cosmos DB only when the Cosmos account is configured to use the MongoDB API. The SQL API is not compatible with the MongoDB driver and therefore not supported.

_Important: this project includes a specialized options type, `CosmosStorageOptions` (in `Hangfire.Mongo.CosmosDB`), which adjusts a number of settings that are required or recommended for Cosmos DB. Use `CosmosStorageOptions` instead of `MongoStorageOptions` when targeting Cosmos DB._

Key overrides in `CosmosStorageOptions`
- CheckQueuedJobsStrategy = Poll
  - Cosmos DB does not reliably support change streams or tailable capped collections in the same way as a regular MongoDB server; polling is the safe strategy.
- CheckConnection = false
  - Cosmos DB's connection semantics and the way it handles metadata can make the generic startup connection check unsuitable; the Cosmos-specific options disable the default connection ping.
- SupportsCappedCollection = false
  - Cosmos DB (Mongo API) does not support capped collections — tailing a notifications collection is not available.
- MigrationLockTimeout = 2 minutes
  - Increased timeout to accommodate Cosmos DB's operational latencies.
- Factory = new CosmosFactory()
  - A Cosmos-specific factory is used to create storage components tuned for Cosmos behavior.
- UtcDateTimeStrategies = [ new IsMasterUtcDateTimeStrategy() ]
  - The UTC date/time strategy is tuned for Cosmos' server responses; this replaces the default set of strategies.

> ⚠️ **Note on testing and support**
>
> **Because access to Azure Cosmos DB (MongoDB API) is limited in the project's test environment, the Cosmos-specific configuration and code paths are not as exhaustively tested as the standard MongoDB implementation. If you use Cosmos DB and encounter issues, please open an issue or submit a PR — community feedback and contributions are appreciated and will help improve compatibility. I will rely on community help to identify and fix provider-specific bugs that cannot be validated in the project's CI/test environment.**

_Note on testing and support_

_Because access to Azure Cosmos DB (MongoDB API) is limited in the project's test environment, the Cosmos-specific configuration and code paths are not as exhaustively tested as the standard MongoDB implementation. If you use Cosmos DB and encounter issues, please open an issue or submit a PR — community feedback and contributions are appreciated and will help improve compatibility. I will rely on community help to identify and fix provider-specific bugs that cannot be validated in the project's CI/test environment._

Example — recommended Cosmos setup

```csharp
using Hangfire.Mongo.CosmosDB;

var mongoUrl = new MongoUrl("mongodb://<user>:<password>@<your-account>.mongo.cosmos.azure.com:10255/?ssl=true&replicaSet=globaldb");
var client = new MongoClient(mongoUrl.ToMongoUrl());

var options = new CosmosStorageOptions
{
    // CosmosStorageOptions already sets recommended defaults for Cosmos
    // You can still tweak other options here if necessary (timeouts, prefix, migration options...)
    Prefix = "hangfire",
    MigrationOptions = new MongoMigrationOptions
    {
        MigrationStrategy = new MigrateMongoMigrationStrategy(),
        BackupStrategy = new CollectionMongoBackupStrategy()
    }
};

// ASP.NET Core pattern: use the Hangfire configuration lambda and call the Cosmos-specific
// extension method `UseCosmosStorage` provided in `CosmosBootstrapperConfigurationExtensions`.
// This registers the storage with Hangfire and returns the created `CosmosStorage` instance.
services.AddHangfire(cfg => cfg.UseCosmosStorage(client, "<database>", options));
services.AddHangfireServer();
```

```csharp
// Non-ASP.NET Core / GlobalConfiguration pattern:
using Hangfire;
using Hangfire.Mongo.CosmosDB;

// Register Cosmos storage on the global Hangfire configuration and capture the returned storage
var storage = GlobalConfiguration.Configuration.UseCosmosStorage(client, "<database>", options);

// Create a server that uses the registered storage
using var server = new BackgroundJobServer(storage);
```

Notes:
- The `UseCosmosStorage` extension method lives in the `Hangfire.Mongo.CosmosDB` namespace; add `using Hangfire.Mongo.CosmosDB;` to access it.
- The extension wraps construction of `CosmosStorage`, registers it on the Hangfire global configuration, and returns the created storage instance so you can use it directly when creating a `BackgroundJobServer` if needed.

Implications and guidance
- Do not rely on change-stream based notifications or tailable collections with Cosmos — the `Poll` strategy is used by default in `CosmosStorageOptions`.
- `CheckConnection` is disabled by default for Cosmos; if you enable it you'll need Cosmos-specific checks and longer timeouts (not recommended).
- Because `SupportsCappedCollection` is false, `TailNotificationsCollection` is not a valid `CheckQueuedJobsStrategy` for Cosmos.
- `CosmosFactory` is used to wire Cosmos-specific implementations (e.g., connections, write semantics); if you need to customize behavior, consider subclassing `CosmosFactory` rather than the base `MongoFactory`.
- UTC date/time handling for Cosmos is tuned via `IsMasterUtcDateTimeStrategy` — if you replace it, ensure any custom strategy matches Cosmos' server response behavior.

### DocumentDB (AWS DocumentDB / Mongo-compatible)

This repository provides a `DocumentDbStorage` path for Mongo-compatible DocumentDB servers (for example AWS DocumentDB). This is intended for MongoDB-compatible services that restrict certain admin commands or have slightly different server responses compared to a full MongoDB server.

_Note on testing and support_

_DocumentDB-compatible providers (such as AWS DocumentDB) are similarly less well-tested in this project due to limited access to those managed services during development. The `DocumentDbStorageOptions` and DocumentDB-specific code paths have been designed to be conservative, but if you find bugs or provider-specific issues please report them or contribute fixes — community help is essential for robust support. I will rely on community contributions and reports to discover and resolve provider-specific issues that cannot be exercised in the project's automated tests._

> ⚠️ **Note on testing and support**
>
> **DocumentDB-compatible providers (such as AWS DocumentDB) are similarly less well-tested in this project due to limited access to those managed services during development. The `DocumentDbStorageOptions` and DocumentDB-specific code paths have been designed to be conservative, but if you find bugs or provider-specific issues please report them or contribute fixes — community help is essential for robust support. I will rely on community contributions and reports to discover and resolve provider-specific issues that cannot be exercised in the project's automated tests.**

Use `DocumentDbStorageOptions` (in `Hangfire.Mongo.DocumentDB`) when targeting DocumentDB-compatible services. The `DocumentDbStorageOptions` constructor narrows the UTC date/time strategies to use `IsMasterUtcDateTimeStrategy`, because unprivileged users on these services may not be able to run higher-privileged commands used by other strategies.

Key points about `DocumentDbStorageOptions`
- UtcDateTimeStrategies = [ new IsMasterUtcDateTimeStrategy() ]
  - Restricts date/time probing to the `isMaster` command which is generally available to unprivileged users on DocumentDB implementations.
- Other storage options retain the defaults from `MongoStorageOptions` unless you override them.

How to use

```csharp
using Hangfire.Mongo.DocumentDB;

var client = new MongoClient("mongodb://<user>:<password>@<your-docdb-host>:27017/?ssl=true");

var options = new DocumentDbStorageOptions
{
    // You can still customize prefix, migration options, and other MongoStorageOptions members
    Prefix = "hangfire",
    MigrationOptions = new MongoMigrationOptions
    {
        MigrationStrategy = new MigrateMongoMigrationStrategy(),
        BackupStrategy = new CollectionMongoBackupStrategy()
    }
};

// ASP.NET Core pattern
services.AddHangfire(cfg => cfg.UseDocumentDbStorage(client, "<database>", options));
services.AddHangfireServer();

// Non-ASP.NET Core / GlobalConfiguration pattern
var storage = GlobalConfiguration.Configuration.UseDocumentDbStorage(client, "<database>", options);
using var server = new BackgroundJobServer(storage);
```

Notes and guidance
- `UseDocumentDbStorage` is implemented in `DocumentDbBootstrapperConfigurationExtensions` (namespace `Hangfire.Mongo.DocumentDB`). Add `using Hangfire.Mongo.DocumentDB;` to access it.
- DocumentDB-compatible services may require TLS/SSL and specific MongoDB driver settings; ensure `MongoClientSettings` are tuned for your provider (timeouts, retry policy, TLS settings).
- Because `DocumentDbStorageOptions` narrows UTC probing to `isMaster`, it is safer to run with unprivileged users. If you need a different strategy, provide a custom `UtcDateTimeStrategy` but test carefully against your provider.
- If your provider exposes additional incompatibilities (capped collections, change streams, etc.), adjust `MongoStorageOptions` flags (for example `SupportsCappedCollection`) or use `CheckQueuedJobsStrategy = CheckQueuedJobsStrategy.Poll` when change-streams/tailable collections are not available.
- If you need provider-specific creation/wiring logic, consider subclassing the provided `DocumentDbStorage` or `CosmosFactory`/`MongoFactory` patterns as appropriate.

### Extending the library

The project provides well-known extension points for advanced customization. Two common extension points are `MongoFactory` and the UTC date/time strategies.

_Note: most classes and methods in this library are public and many are virtual (for example `MongoFactory`, `MongoWriteOnlyTransaction`, `MongoConnection` and related components). This design allows you to subclass and override behaviour at many points — you can swap internal components, change commit behavior, alter notification logic, or plug-in custom serialization by overriding the appropriate virtual methods._

_In short: almost all methods are public and many are virtual, so you can change almost any behaviour by subclassing and overriding the provided components._

1) Overriding `MongoFactory`

`MongoFactory` is the place where MongoDB collections, indexes and other components are created. By providing a custom implementation you can:
- Create custom indexes or collection options,
- Plug-in custom DTO serialization or mapping,
- Swap collection implementations for testing.

Accurate examples (based on the real `MongoFactory` API):

```csharp
// Example 1: override the database context creation to enforce a custom prefix
public class CustomMongoFactory : MongoFactory
{
    public override HangfireDbContext CreateDbContext(IMongoClient mongoClient, string databaseName, string prefix)
    {
        // Force a different prefix for all Hangfire collections
        var enforcedPrefix = "myapp.hangfire";
        return base.CreateDbContext(mongoClient, databaseName, enforcedPrefix);
    }
}
```

```csharp
// Example 2: override the distributed lock creation to change resource naming (or add instrumentation)
public class CustomMongoFactoryWithLocks : MongoFactory
{
    public override MongoDistributedLock CreateMongoDistributedLock(string resource, TimeSpan timeout, HangfireDbContext dbContext, MongoStorageOptions storageOptions)
    {
        // Use an application-specific prefix for the lock resource name
        var customResource = $"MyAppLock:{resource}";

        // You could also wrap the returned lock with your own implementation that adds logging/metrics
        return new MongoDistributedLock(customResource, timeout, dbContext, storageOptions);
    }
}
```

Wiring a custom factory
- `MongoStorageOptions` exposes a `Factory` property. Assign your custom factory before creating `MongoStorage` or before calling `UseMongoStorage`:

```csharp
var options = new MongoStorageOptions
{
    Factory = new CustomMongoFactory(),
};

services.AddHangfire(cfg => cfg.UseMongoStorage(mongoClient, "mydb", options));
```

If you need to customize other behaviors (job fetching, notifications, expiration manager etc.), inspect the available virtual methods on `MongoFactory` and override the appropriate creation method (for example `CreateMongoJobFetcher`, `CreateMongoNotificationObserver`, `CreateMongoExpirationManager`).

2) Custom UTC date/time strategies

Date/time serialization is important for cross-platform correctness and compatibility with various MongoDB servers. The library exposes swappable UTC strategies (look for implementations under `UtcDateTime` or similar namespaces) so you can control how DateTime values are serialized and deserialized.

Example custom strategy:

```csharp
public class CustomUtcDateTimeStrategy : UtcDateTimeStrategy
{
    public override BsonValue Serialize(DateTime dateTime)
    {
        // Force DateTime to UTC and store as BsonDateTime
        return new BsonDateTime(DateTime.SpecifyKind(dateTime, DateTimeKind.Utc));
    }

    public override DateTime Deserialize(BsonValue value)
    {
        return value.AsBsonDateTime.ToUniversalTime();
    }
}
```

Wiring the strategy
- Set `MongoStorageOptions.UtcDateTimeStrategies` to an array of the strategies you want to use (in order of preference). This property is an array of `UtcDateTimeStrategy` instances and should be configured before creating `MongoStorage` / calling `UseMongoStorage`.

Example:

```csharp
var options = new MongoStorageOptions
{
    UtcDateTimeStrategies = new UtcDateTimeStrategy[]
    {
        new CustomUtcDateTimeStrategy(),
        new AggregationUtcDateTimeStrategy(),
        new ServerStatusUtcDateTimeStrategy()
    }
};

services.AddHangfire(cfg => cfg.UseMongoStorage(mongoClient, "mydb", options));
```

- Alternative: if you need lower-level control (for example registering Bson serializers or setting up class maps) you can wire the strategy inside a custom `MongoFactory` implementation — the factory is invoked when the storage constructs its internal components, so it can be used to ensure serializers and mappings are registered before collections are used.

Example — customize `MongoWriteOnlyTransaction`

A deeper extension point is `MongoWriteOnlyTransaction`. You can subclass it to alter commit behavior, add retries, instrumentation or change how notifications are signalled. Below is a compact example that:
- Subclasses `MongoWriteOnlyTransaction` and overrides `ExecuteCommit` to add a retry loop,
- Supplies the custom transaction from a custom `MongoFactory`, and
- Wires the factory via `MongoStorageOptions.Factory`.

```csharp
using System;
using System.Collections.Generic;
using System.Threading;
using MongoDB.Bson;
using MongoDB.Driver;
using Hangfire.Mongo.Database;

// 1) Custom transaction with a simple retry around the bulk commit
public class CustomMongoWriteOnlyTransaction : MongoWriteOnlyTransaction
{
    public CustomMongoWriteOnlyTransaction(HangfireDbContext dbContext, MongoStorageOptions storageOptions)
        : base(dbContext, storageOptions)
    {
    }

    protected override void ExecuteCommit(IMongoCollection<BsonDocument> jobGraph, List<WriteModel<BsonDocument>> writeModels, BulkWriteOptions bulkWriteOptions)
    {
        const int maxAttempts = 3;
        int attempt = 0;

        while (true)
        {
            try
            {
                // use base behavior for actual bulk write
                base.ExecuteCommit(jobGraph, writeModels, bulkWriteOptions);
                return;
            }
            catch (MongoException) when (++attempt < maxAttempts)
            {
                // simple backoff; replace with your preferred retry policy or instrumentation
                Thread.Sleep(200 * attempt);
            }
        }
    }
}

// 2) Custom factory that returns the custom transaction
public class CustomMongoFactory : MongoFactory
{
    public override MongoWriteOnlyTransaction CreateMongoWriteOnlyTransaction(HangfireDbContext dbContext, MongoStorageOptions storageOptions)
    {
        return new CustomMongoWriteOnlyTransaction(dbContext, storageOptions);
    }
}

// 3) Wiring via options
var options = new MongoStorageOptions
{
    Factory = new CustomMongoFactory()
};

services.AddHangfire(cfg => cfg.UseMongoStorage(mongoClient, "mydb", options));
```

Notes
- You can override other virtual methods on `MongoWriteOnlyTransaction` (for example `SignalJobsAddedToQueues` or `Log`) to change notifications or debug output.
- Use a custom factory when you need to swap multiple internal components; override several `Create...` methods as needed.

### Migration and backups

The library supports migration strategies to handle schema changes between releases. Choose the strategy that fits your operational needs:
- Throw (default): refuse to start when a schema version mismatch is detected.
- Drop: drop Hangfire collections and recreate schema from scratch (data loss).
- Migrate: attempt to migrate data forward. May not preserve all data — test carefully.

Backup strategies
- None: do not perform backups before migration.
- Collection clone: copy collections within the database before applying migrations.
- Custom: implement `MongoBackupStrategy` to provide a bespoke backup mechanism (e.g., export to files or another database).

Example configuration snippet:

```csharp
var migrationOptions = new MongoMigrationOptions
{
    MigrationStrategy = new MigrateMongoMigrationStrategy(),
    BackupStrategy = new CollectionMongoBackupStrategy()
};

var storageOptions = new MongoStorageOptions
{
    MigrationOptions = migrationOptions,
    InvisibilityTimeout = TimeSpan.FromMinutes(30)
};

GlobalConfiguration.Configuration.UseMongoStorage("<connection string with database name>", storageOptions);
```

Naming conventions

Hangfire.Mongo enforces PascalCase for its internal collections and will ignore application-wide convention packs (such as CamelCaseElementNameConvention) for Hangfire collections. This ensures schema stability across applications using different conventions.

Features summary
- Durable job and state storage in MongoDB.
- Multiple queue notification strategies: change streams (Watch), polling (Poll), and tailable notifications (TailNotificationsCollection).
- Schema migration and configurable backup strategies.
- Pluggable `MongoFactory` for customizing collections, indexes and serializers.
- Swappable UTC date/time strategies for fine-grained date handling.
- Configurable collection prefixing and connection checks.
- Compatible with MongoDB and MongoDB-compatible services (including Cosmos DB using the MongoDB API).

Contributing

Contributions are welcome. When submitting changes:
- Add tests for new behavior (if applicable).
- Document breaking changes and migration steps.
- Include migration/backup code when modifying schema.

Contributors

- Sergey Zwezdin ([@sergeyzwezdin](https://github.com/sergeyzwezdin))
- Martin Løbger ([@marloe](https://github.com/marloe))
- Jonas Gottschau ([@gottscj](https://github.com/gottscj))

License

Hangfire.Mongo is released under the MIT License. See the LICENSE file for details.
