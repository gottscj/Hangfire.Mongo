Hangfire.Mongo
==============

[![Build status](https://ci.appveyor.com/api/projects/status/xjr953s29pwwsuq4/branch/master?svg=true)](https://ci.appveyor.com/project/sergun/hangfire-mongo/branch/master)

MongoDB support for [Hangfire](http://hangfire.io/) library. By using this library you can store all jobs information in MongoDB.

**Note:** This is pre-alpha version, that has not been ever tested. Please, use it with care :)

# Installation

To install Hangfire MongoDB Storage, run the following command in the Nuget Package Manager Console:

```
PM> Install-Package HangFire.Mongo
```

# Usage

```csharp
app.UseHangfire(config =>
{
	config.UseMongoStorage("<connection string>", "<database name>");
});
```

For example:

```csharp
app.UseHangfire(config =>
{
	config.UseMongoStorage("mongodb://localhost", "ApplicationDatabase");
});
```

## Custom collections prefix

To use custom prefix for collections names specify it on Hangfire setup:

```csharp
app.UseHangfire(config =>
{
	config.UseMongoStorage("mongodb://localhost", "ApplicationDatabase",
  	  	new MongoStorageOptions { Prefix = "custom" } );
});
```

License
-------

Hangfire.Mongo is released under the [MIT License](https://raw.githubusercontent.com/sergun/Hangfire.Mongo/master/LICENSE).
