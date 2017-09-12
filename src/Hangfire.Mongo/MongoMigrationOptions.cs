using System;

namespace Hangfire.Mongo
{

    /// <summary>
    /// The supported migration strategies.
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
    /// The supported backup strategies during migration.
    /// </summary>
    public enum MongoBackupStrategy
    {
        /// <summary>
        /// No backup is made before migration.
        /// </summary>
        None,

        /// <summary>
        /// A collection-by-collection backup is made to the same database.
        /// Recommended to use this if you store the Hangfire jobs in the 
        /// same database as your actual application data.
        /// This is the prefered and default backup strategy.
        /// </summary>
        Collections,

        /// <summary>
        /// Will copy the entire database into a new databse.
        /// When using this backup strategy, access to the
        /// "admin" database is required.
        /// </summary>
        Database
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
            BackupStrategy = MongoBackupStrategy.Collections;
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
        [Obsolete("Please use '" + nameof(BackupStrategy) + "' instead. This will be removed in next release.")]
        public bool Backup
        {
            get => BackupStrategy != MongoBackupStrategy.None;
            set
            {
                if (value != Backup)
                {
                    BackupStrategy = value ? MongoBackupStrategy.Collections : MongoBackupStrategy.None;
                }
            }
        }


        /// <summary>
        /// The backup strategy to use before migrating.
        /// </summary>
        public MongoBackupStrategy BackupStrategy { get; set; }


        /// <summary>
        /// Collection backup name postfix for all Hangfire related collections.
        /// </summary>
        /// <remarks>
        /// The format for the backed up collection name is:
        /// {collection-name}.{schema-version}.{BackupPostfix}
        /// The format for the backed up database name is:
        /// {database-name}-{schema-version}-{BackupPostfix}
        /// </remarks>
        public string BackupPostfix { get; set; }
    }         
}