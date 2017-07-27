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
    /// Only forward migration is suported
    /// </summary>
    public class MongoMigrationOptions
    {

        /// <summary>
        /// Constructs migration options with default parameters
        /// </summary>
        public MongoMigrationOptions()
            : this(MongoMigrationStrategy.None)
        {
        }

        /// <summary>
        /// Constructs migration options with specific strategy
        /// </summary>
        /// <param name="strategy">The migration strategy to use</param>
        public MongoMigrationOptions(MongoMigrationStrategy strategy)
        {
            Strategy = strategy;
        }

        /// <summary>
        /// The strategy used for migration to newer schema versions
        /// </summary>
        public MongoMigrationStrategy Strategy { get; set; }



    }
}