using System;
using Hangfire.Mongo.Migration.Steps;

namespace Hangfire.Mongo.Migration
{
    /// <summary>
    /// Represents exceptions for migrations
    /// </summary>
#if !NETSTANDARD1_5
    [Serializable]
#endif
    internal class MongoMigrationException : Exception
    {

        /// <summary>
        /// Initializes a new instance of the <see cref="T:Hangfire.Mongo.Migration.MongoMigrationException"/> class.
        /// Generates the message based on the <paramref name="migrationStep"/>
        /// </summary>
        /// <param name="migrationStep">
        /// The migration step that failed.
        /// </param>
        public MongoMigrationException(IMongoMigrationStep migrationStep)
            : base($@"Migration failed in {migrationStep.GetType().FullName}")
        {
        }


        /// <summary>
        /// Initializes a new instance of the <see cref="T:Hangfire.Mongo.Migration.MongoMigrationException"/> class.
        /// Generates the message based on the <paramref name="migrationStep"/>
        /// </summary>
        /// <param name="migrationStep">
        /// The migration step that failed.
        /// </param>
        /// <param name="message">Exception message</param>
        public MongoMigrationException(IMongoMigrationStep migrationStep, string message)
            : base($@"Migration failed in {migrationStep.GetType().FullName} - {message}")
        {
        }


        /// <summary>
        /// Initializes a new instance of the <see cref="T:Hangfire.Mongo.Migration.MongoMigrationException"/> class.
        /// Generates the message based on the <paramref name="migrationStep"/>
        /// </summary>
        /// <param name="migrationStep">
        /// The migration step that failed.
        /// </param>
        /// <param name="innerException">
        /// The inner exception.
        /// </param>
        public MongoMigrationException(IMongoMigrationStep migrationStep, Exception innerException)
            : base($@"Migration failed in {migrationStep.GetType().FullName}", innerException)
        {
        }


        /// <summary>
        /// Initializes a new instance of the <see cref="T:Hangfire.Mongo.Migration.MongoMigrationException"/> class.
        /// </summary>
        /// <param name="message">Exception message</param>
        public MongoMigrationException(string message)
            : base(message)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="T:Hangfire.Mongo.Migration.MongoMigrationException"/> class.
        /// </summary>
        /// <param name="message">Exception message</param>
        /// <param name="innerException">Inner exception</param>
        public MongoMigrationException(string message, Exception innerException)
            : base(message, innerException)
        {
        }

    }
}