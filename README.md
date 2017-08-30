Hangfire.Mongo
==============

[![Build status](https://ci.appveyor.com/api/projects/status/xjr953s29pwwsuq4?svg=true)](https://ci.appveyor.com/project/sergeyzwezdin/hangfire-mongo) [![Nuget version](https://img.shields.io/nuget/v/Hangfire.Mongo.svg)](https://www.nuget.org/packages/Hangfire.Mongo)

MongoDB support for [Hangfire](http://hangfire.io/) library. By using this library you can store all jobs information in MongoDB.

# Installation

To install Hangfire MongoDB Storage, run the following command in the Nuget Package Manager Console:

```
PM> Install-Package Hangfire.Mongo
```

# Usage

```csharp
public void Configuration(IAppBuilder app)
{
    GlobalConfiguration.Configuration.UseMongoStorage("<connection string>", "<database name>");

    app.UseHangfireServer();
    app.UseHangfireDashboard();
}
```

For example:

```csharp
public void Configuration(IAppBuilder app)
{
    GlobalConfiguration.Configuration.UseMongoStorage("mongodb://localhost", "ApplicationDatabase");

    app.UseHangfireServer();
    app.UseHangfireDashboard();
}
```

## Custom collections prefix

To use custom prefix for collections names specify it on Hangfire setup:

```csharp
public void Configuration(IAppBuilder app)
{
    GlobalConfiguration.Configuration.UseMongoStorage("<connection string>", "<database name>",
        new MongoStorageOptions { Prefix = "custom" } );

    app.UseHangfireServer();
    app.UseHangfireDashboard();
}
```

## Custom Mongo DB settings

To use custom Mongo DB connection settings you can use `MongoClientSettings` object from Mongo DB driver package.
In this case just use it instead of passing connection string when you configure your storage.

```csharp
public void Configuration(IAppBuilder app)
{
    GlobalConfiguration.Configuration.UseMongoStorage(new MongoClientSettings()
            {
                // ...
                IPv6 = true
            }, "ApplicationDatabase");

    app.UseHangfireServer();
    app.UseHangfireDashboard();
}
```

## Migration

We sometimes introduce breaking changes in the schema. For this reason we have introduced migration.
Three migration strategies exists.
- None

  This is the default migration strategy. It will throw an InvalidOperationException never letting you get up and running if there is a schema version mismatch. So it forces you to decide what migration strategy is best for you and at the same time keeps your data safe.
- Drop

  This will simply just drop your existing Hangfire.Mongo database and update the schema version. No fuzz and ready to start from scratch.
  It is the perfect strategy if you e.g. enque all your jobs at startup.
- Migrate

  This will migrate your database from one schema version to the next until the required schema version is reached. Chances are that not all data can be migrated, why some loss of data might occur. Please use with caution and thougoughly test before deploying to production. We are not responsible for data loss.

  NOTE: Only forward migration is supported. If you need to revert to a previous schema version, you need to **manually** drop or restore the previous database.

```csharp
public void Configuration(IAppBuilder app)
{
    var migrationOptions = new MongoMigrationOptions
    {
        Strategy = MongoMigrationStrategy.Migrate
    };
    var storageOptions = new MongoStorageOptions
    {
        // ...
        MigrationOptions = migrationOptions
    };
    GlobalConfiguration.Configuration.UseMongoStorage("<connection string>", "<database name>", storageOptions);

    app.UseHangfireServer();
    app.UseHangfireDashboard();
}
```

### Migration Backup
By default a backup is made before attempting to migrate.
Backup consists of "cloning" each collection to the same database.
You can disable or customize the backup using MongoMigrationOptions.
NOTE: This software is made by humans in our sparetime - we do our best but are not responsible for data loss.

Contributors
------------

- Sergey Zwezdin ([@sergeyzwezdin](https://github.com/sergeyzwezdin))
- Martin Løbger ([@marloe](https://github.com/marloe))
- Jonas Gottschau ([@gottscj](https://github.com/gottscj))

License
-------

Hangfire.Mongo is released under the [MIT License](https://raw.githubusercontent.com/sergun/Hangfire.Mongo/master/LICENSE).
