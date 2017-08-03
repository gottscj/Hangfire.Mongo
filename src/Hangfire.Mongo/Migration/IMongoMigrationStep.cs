﻿using MongoDB.Driver;

namespace Hangfire.Mongo.Migration
{

    /// <summary>
    /// Mongo migration step.
    /// </summary>
    internal interface IMongoMigrationStep
    {
        /// <summary>
        /// The schema this migration step targets.
        /// </summary>
        MongoSchema TargetSchema { get; }

        /// <summary>
        /// Specifies the order migration steps for the same schema is executed.
        /// </summary>
        long Sequence { get; }

		/// <summary>
		/// Executes the migration step.
		/// </summary>
		/// <param name="database">The mongo database.</param>
		/// <param name="storageOptions">Storage options.</param>
		/// <returns>True on success, else false</returns>
		bool Execute(IMongoDatabase database, MongoStorageOptions storageOptions);
    }
}