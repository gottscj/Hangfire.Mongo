using System.Collections.Generic;
using System.Linq;
using Hangfire.Mongo.Database;
using MongoDB.Driver;

namespace Hangfire.Mongo.Migration.Strategies
{
    internal abstract class MongoMigrationStrategyBase : IMongoMigrationStrategy
    {
        protected readonly HangfireDbContext _dbContext;
        protected readonly MongoStorageOptions _storageOptions;

        public MongoMigrationStrategyBase(HangfireDbContext dbContext, MongoStorageOptions storageOptions)
        {
            _dbContext = dbContext;
			_storageOptions = storageOptions;
		}

        public abstract void Migrate(MongoSchema fromSchema, MongoSchema toSchema);


		/// <summary>
		/// Find hangfire collection namespaces by reflecting over properties on database.
		/// </summary>
        protected IEnumerable<string> ExistingHangfireCollectionNameSpaces(MongoSchema schema)
		{
			var existingCollectionNames = ExistingDatabaseCollectionNames().ToList();
			return schema.CollectionNames(_storageOptions.Prefix).Where(c => existingCollectionNames.Contains(c));
		}


		/// <summary>
		/// Gets the existing collection names from database
		/// </summary>
		protected IEnumerable<string> ExistingDatabaseCollectionNames()
		{
            return _dbContext.Database.ListCollections().ToList().Select(c => c["name"].AsString);
		}


		/// <summary>
		/// Generate the name of tha tbackup collection based on the original collection name and schema.
		/// </summary>
		protected string BackupCollectionName(string collectionName, MongoSchema schema)
		{
			return $@"{collectionName}.{(int)schema}.{_storageOptions.MigrationOptions.BackupPostfix}";
		}


	}
}
