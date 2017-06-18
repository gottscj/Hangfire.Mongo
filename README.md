Hangfire.Mongo
==============

[![Build status](https://ci.appveyor.com/api/projects/status/xjr953s29pwwsuq4?svg=true)](https://ci.appveyor.com/project/sergeyzwezdin/hangfire-mongo) [![Nuget version](https://img.shields.io/nuget/v/Hangfire.Mongo.svg)](https://www.nuget.org/packages/HangFire.Mongo)

MongoDB support for [Hangfire](http://hangfire.io/) library. By using this library you can store all jobs information in MongoDB.


## What's New (06/18/2017)

### *** BREAKING CHANGES FOR v0.4.0 ***
- Combined collections for state data into one collection
- Optimized job creation
  - Not getting timestamp from mongodb. Using Datetime.UtcNow
  - Using MongoDB native "ObjectId" as JobId instead of int.

### Why did you do this?
We currently have issues regarding atomicity in our "JobStorageTransaction" implemention. 
In order to address this we are combining this information into one collection in order to do bulk writes.
(We have not yet fixed our "JobStorageTransaction", but we will addres those for next release)

### What should I do?
You have to drop your jobs db.

### Other changes/fixes
- Fix MongoStorage.ToString() when settings contain multiple servers
- Upgraded to VS2017, new csproj and MSBuild
- Fix for duplicated key error writing schema version
- Update to JobDto, added parameters and statehistory to JobDto

### Whats next
 - Fixes for Hangfire.Pro features
 - ReactiveMongoStorage utilizing capped collections, no need to poll.
 
# Installation

To install Hangfire MongoDB Storage, run the following command in the Nuget Package Manager Console:

```
PM> Install-Package HangFire.Mongo
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

Contrubutors
------------

* Sergey Zwezdin ([@sergeyzwezdin](https://github.com/sergeyzwezdin))
* Martin Løbger ([@marloe](https://github.com/marloe))
* Jonas Gottschau ([@gottscj](https://github.com/gottscj))

License
-------

Hangfire.Mongo is released under the [MIT License](https://raw.githubusercontent.com/sergun/Hangfire.Mongo/master/LICENSE).
