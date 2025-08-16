using System;
using Hangfire.Mongo.Migration;
using Hangfire.Mongo.Migration.Strategies;
using Hangfire.Mongo.Migration.Strategies.Backup;

namespace Hangfire.Mongo
{
    /// <summary>
    /// Represents options for MongoDB migration.
    /// Only forward migration is supported.
    /// </summary>
    public class MongoMigrationOptions
    {
        private MongoMigrationFactory _mongoMigrationFactory;
        private MongoBackupStrategy _backupStrategy;
        private MongoMigrationStrategy _migrationStrategy;
        private string _backupPostfix;

        /// <summary>
        /// Constructs migration options with default parameters.
        /// </summary>
        public MongoMigrationOptions()
        {
            BackupStrategy = new NoneMongoBackupStrategy();
            MigrationStrategy = new ThrowMongoMigrationStrategy();
            MongoMigrationFactory = new MongoMigrationFactory();
            BackupPostfix = "migrationbackup";
        }

        /// <summary>
        /// Factory which creates migration steps
        /// </summary>
        public MongoMigrationFactory MongoMigrationFactory
        {
            get => _mongoMigrationFactory;
            set
            {
                if (value == null)
                {
                    throw new ArgumentException($"'{MongoMigrationFactory}' cannot be null");
                }
                _mongoMigrationFactory = value;
            }
        }

        /// <summary>
        /// Backup strategy
        /// </summary>
        public MongoBackupStrategy BackupStrategy
        {
            get => _backupStrategy;
            set
            {
                if (value == null)
                {
                    throw new ArgumentException($"'{BackupStrategy}' cannot be null");
                }
                _backupStrategy = value;
            }
        }

        /// <summary>
        /// Migration strategy
        /// </summary>
        public MongoMigrationStrategy MigrationStrategy
        {
            get => _migrationStrategy;
            set
            {
                if (value == null)
                {
                    throw new ArgumentException($"'{MigrationStrategy}' cannot be null");
                }
                _migrationStrategy = value;
            }
        }

        /// <summary>
        /// Collection backup name postfix for all Hangfire related collections.
        /// </summary>
        /// <remarks>
        /// The format for the backed up collection name is:
        /// {collection-name}.{schema-version}.{BackupPostfix}
        /// The format for the backed up database name is:
        /// {database-name}-{schema-version}-{BackupPostfix}
        /// </remarks>
        public string BackupPostfix
        {
            get => _backupPostfix;
            set
            {
                if (value == null)
                {
                    throw new ArgumentException($"'{BackupPostfix}' cannot be null");
                }
                _backupPostfix = value;
            }
        }
    }
}