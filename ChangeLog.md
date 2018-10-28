
## What's New (28/10/2018)

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
 
