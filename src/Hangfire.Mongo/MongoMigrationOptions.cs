using System;

namespace Hangfire.Mongo
{

    /// <summary>
    /// The differnet supported migration strategies.
    /// </summary>
    public enum MongoMigrationStrategy
    {
        /// <summary>
        /// Do not migrate. You are on your own, we will not validate the schema at all.
        /// </summary>
        None,

        /// <summary>
        /// Drops the entire database, if schema version is increased.
        /// </summary>
        Drop,

        /// <summary>
        /// Migrate from old schema to new one.
        /// </summary>
        Migrate
    }


    /// <summary>
    /// Represents options for MongoDB migration.
    /// Only forward migration is suported.
    /// </summary>
    public class MongoMigrationOptions
    {

        /// <summary>
        /// Constructs migration options with default parameters.
        /// </summary>
        public MongoMigrationOptions()
            : this(MongoMigrationStrategy.None)
        {
        }

        /// <summary>
        /// Constructs migration options with specific strategy.
        /// </summary>
        /// <param name="strategy">The migration strategy to use</param>
        public MongoMigrationOptions(MongoMigrationStrategy strategy)
        {
            Strategy = strategy;
            Backup = true;
            BackupPostfix = "migrationbackup";
        }


        /// <summary>
        /// The strategy used for migration to newer schema versions.
        /// </summary>
        public MongoMigrationStrategy Strategy { get; set; }


        /// <summary>
        /// If true, a backup of all Hangfire.Mongo collection will be
        /// performed before any migration starts.
        /// </summary>
        public bool Backup { get; set; }


        /// <summary>
        /// Collection backup name postfix for all Hangfire related collections.
        /// </summary>
        /// <remarks>
        /// The format for the backed up collection name is:
        /// {collection-name}.{schema-version}.{BackupPostfix}
        /// </remarks>
        public string BackupPostfix { get; set; }
    }         
}