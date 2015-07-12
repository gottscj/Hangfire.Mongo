Hangfire.Mongo
==============

[![Build status](https://ci.appveyor.com/api/projects/status/xjr953s29pwwsuq4/branch/master?svg=true)](https://ci.appveyor.com/project/sergun/hangfire-mongo/branch/master) [![Nuget version](https://img.shields.io/nuget/v/Hangfire.Mongo.svg)](https://www.nuget.org/packages/HangFire.Mongo)

MongoDB support for [Hangfire](http://hangfire.io/) library. By using this library you can store all jobs information in MongoDB.

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

License
-------

Hangfire.Mongo is released under the [MIT License](https://raw.githubusercontent.com/sergun/Hangfire.Mongo/master/LICENSE).
