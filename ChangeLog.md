
## What's New (07/24/2017)

### Bug fixes (v0.4.2)
- MonitoringApi returning succeeded jobs when quering processing
- List and Set not sorted correctly

### Bug fixes (v0.4.1)
- Add workaround for MongDB C# driver not adding inheritance types when doing upsert
  - (We have filed a bug report with MongoDB)
- Fix bug in MongoWriteOnlyTransaction.AddRangeToSet where Value not being written for upsert

### *** BREAKING CHANGES FOR v0.4.0 *** (v0.4.0)
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
 
