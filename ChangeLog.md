
## What's New (Dec. 11th. 2019)

### v0.6.6
- Add support for specifying the database name in the connection string (#219)
- InvisibilityTimeout should not be obsolete (#220)

### v0.6.5
- Cleanup expired DistributedLock after each wait iteration (#217)
- Make extracting mongodb version more resilient

### v0.6.4
- Upgrade to "Hangfire.Core" Version="1.7.6"
- Upgrade to "MongoDB.Bson" Version="2.9.1"
- Upgrade to "MongoDB.Driver" Version="2.9.1"
- Upgrade to "MongoDB.Driver.Core" Version="2.9.1"
- Make sure to use timout when waiting for signalled semaphore #207
- Do not try to delete expired migration lock #208
- Allow user to supply preconfigured MongoClient #199
- Dont restart observing capped collection if collection is not capped (deleted by user) #204
- Fix job set filter - escape regex chars #212

### v0.6.3
- Explicitly set discriminators because filters rely on them (#200)

### v0.6.2
- Handle cancellation more gracefully (#191)
- Make Connection check timeout configurable and set new default = 5 seconds (#191)

### v0.6.1
- Fixed potential race condition releasing distributed lock mutex (#188)

### v0.6.0
- Added connection check when initializing MongoStorage (#179)
- Fixed Jobs Stuck In 'Enqueued' State (#180)
- Added Tailable collection 'notifications' for signalling enqueued jobs and released locks
- Update to latest Hangfire.Core (v1.7.1)
- Update to latest Mongo.Driver (v2.8.0)
- Target net452 as this is required by latest Mongo.Driver
- Added dedicated migration lock.
- Fix old migration step. (only used if migrating from an old schema)
- Enhanced logging

### v0.5.15
- Fix Dashboard, top menu, Recurring Jobs count is 0 (#173) 
- Fix GetAllItemsFromSet truncated values (#175)

### v0.5.14
- Fix race case in distributed lock implementation (#166, #134)
- Not JSON serializing Server data
- Add unique index ('Key') for 'Hash' and 'Counter' data (related to #166)
- Rename 'ListDto' field 'Key' to 'Item' ('Key' is now a unique index) (related to #166)
- Remove obsolete counters, which should have been removed in migrations for schema 13
- Mark ctor's for MongoStorage which takes connectionString obsolete, use 'MongoClientSettings'
- Fix requeued job state can be incorrect when multiple servers process jobs (#167)
- Add console logging per default in samples

### v0.5.13
- Use 'buildinfo' command instead of 'serverStatus' to get the server version. Because the 'buildinfo' command does not require - root privileges.
- Add missing migration for HashDto.Field removal 
- Add data integrity tests
- Update to "MongoDB.Driver" Version="2.7.2"
- Update to "Hangfire.Core" Version="1.6.21"
- Use separate collection for migration locks as the ".locks" collection might be dropped by if the migration strategy is "Drop"
- Making migration support CosmosDB
- Fix if client is using camelcase convention
- Removing Obsolete interfaces and logic
- Obsolete access to DbContext and queue providers
- Fix migration option combi migrationstrategy.drop and backupstrategy.none
- Fix timezone issue with conflicting bsonserializer settings 

### v0.5.12
- Using $dec and $inc operators for counters
- Merging HashDto fields into one document
- Deprecating  direct db access and queueproviders
- Removed use of $slice (#151) 

### v0.5.11
- Fixed duplicate key exception in advanced setups (#70)
- Fixed DeadLock on concurrent environment (#139)
- Update to latest Hangfire
- Update to latest MongoDB

### v0.5.10
- Fix for Hangfire Dashboard History Graph showing incorrect counts
- Update to latest Hangfire
- Update to latest MongoDB

### v0.5.9
- Fix for Hangfire Dashboard History Graph showing incorrect counts
- Added indexes to all our collections
- Updated a few NuGet references
- Cleaned up library references
- Improve migration stability
- Remove dependency to Microsoft.CSharp

### v0.5.8
- Broken release

### v0.5.7
- Fix for broken migration

### v0.5.6
- Job state now shows correct in dashboard
- Hangfire dashboard logs are now sorted descending

### v0.5.5
- Use default naming conventions for our Dto models  
  This should hopefully eliminate all the camelCase issues
- Optimized job aggregation to filter by only the status needed
- Add version check when backing up collections

### v0.5.4
- Fix broken migration

### v0.5.3
- Added new backup database strategy when migration
- Fixed a bug that made backup incompatiple with MongoDB 3.4
- A few code optimizations

### v0.5.2
- Forcing the use of Pascal Casing in Hangfire specific collections.
  See [README.md](https://github.com/sergeyzwezdin/Hangfire.Mongo#naming-convention) for more info.

### v0.5.1
- Fix for migration of stateData collelction.

### v0.5.0
- Migration has been introduces. So now you can upgrade from previous version without loosing you jobs.
- MonitoringApi returning succeeded jobs when quering processing
- List and Set not sorted correctly
- Fixed NuGet references for xUnit so they again can be run from IDE
- Updated Hangfire NuGet reference to latest ([Hangfire 1.6.15](https://github.com/HangfireIO/Hangfire/releases/tag/v1.6.15))
- Updated project files so it is possible to build and run .NETCore sample from [Visual Studio Code](https://code.visualstudio.com)
- Using MongoWriteOnlyTransaction.SetRangeInHash in MongoConnection.SetRangeInHash

### v0.4.1
- Add workaround for MongDB C# driver not adding inheritance types when doing upsert
  - (We have filed a bug report with MongoDB)
- Fix bug in MongoWriteOnlyTransaction.AddRangeToSet where Value not being written for upsert

### v0.4.0 *** BREAKING CHANGES ***
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
 
