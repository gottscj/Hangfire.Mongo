using System;
using System.Linq;
using Hangfire.Mongo.Dto;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Hangfire.Mongo.Migration
{
    /// <summary>
    /// Manages migration from one schema version to the required.
    /// </summary>
    public class MongoMigrationManager
    {
        private readonly MongoStorageOptions _storageOptions;
        private readonly IMongoDatabase _database;

        /// <summary>
        /// Gets required schema based on codebase
        /// </summary>
        public virtual MongoSchema RequiredSchemaVersion =>
            Enum.GetValues(typeof(MongoSchema)).Cast<MongoSchema>().OrderBy(v => v).Last();


        /// <summary>
        /// ctor
        /// </summary>
        /// <param name="storageOptions"></param>
        /// <param name="database"></param>
        public MongoMigrationManager(MongoStorageOptions storageOptions, IMongoDatabase database)
        {
            _storageOptions = storageOptions;
            _database = database;
        }

        /// <summary>
        /// Checks if migration is needed
        /// </summary>
        /// <returns></returns>
        public virtual bool NeedsMigration()
        {
            var currentSchema = GetCurrentSchema(_database);

            if (currentSchema.Version == RequiredSchemaVersion)
            {
                return false;
            }

            return true;
        }
        
        /// <summary>
        /// Runs migrations with given strategies
        /// </summary>
        /// <returns></returns>
        public virtual bool MigrateUp()
        {
            var currentSchema = GetCurrentSchema(_database);

            if (currentSchema.Version == RequiredSchemaVersion)
            {
                return false;
            }
            
            _storageOptions
                .MigrationOptions
                .MigrationStrategy?.ValidateSchema(RequiredSchemaVersion, currentSchema.Version);

            _storageOptions
                .MigrationOptions
                .BackupStrategy?.Backup(_storageOptions, _database, currentSchema.Version, RequiredSchemaVersion);

            _storageOptions
                .MigrationOptions
                .MigrationStrategy?.Execute(_storageOptions, _database, currentSchema.Version, RequiredSchemaVersion);

            return true;
        }

        /// <summary>
        /// Gets current schema from DB
        /// </summary>
        /// <param name="database"></param>
        /// <returns></returns>
        protected virtual SchemaDto GetCurrentSchema(IMongoDatabase database)
        {
            var document = database
                .GetCollection<BsonDocument>(_storageOptions.Prefix + ".schema")
                .Find(new BsonDocument())
                .FirstOrDefault();
            return document == null
                // We do not have a schema version yet
                // - assume an empty database and run full migrations
                ? new SchemaDto {Version = MongoSchema.None}
                : new SchemaDto(document);
        }
    }
}