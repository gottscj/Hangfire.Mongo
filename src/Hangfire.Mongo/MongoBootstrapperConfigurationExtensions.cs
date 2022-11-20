using System;
using MongoDB.Driver;

namespace Hangfire.Mongo
{
    /// <summary>
    /// Represents extensions to configure MongoDB storage for Hangfire
    /// </summary>
    public static class MongoBootstrapperConfigurationExtensions
    {
        /// <summary>
        /// Configure Hangfire to use MongoDB storage
        /// </summary>
        /// <param name="configuration">Configuration</param>
        /// <param name="connectionString">Connection string for Mongo database, for example 'mongodb://username:password@host:port/database'</param>
        /// <remarks>The connection string must include the database name</remarks>
        /// <returns></returns>
        public static IGlobalConfiguration<MongoStorage> UseMongoStorage(this IGlobalConfiguration configuration,
            string connectionString)
        {
            return UseMongoStorage(configuration, connectionString, new MongoStorageOptions());
        }

        /// <summary>
        /// Configure Hangfire to use MongoDB storage
        /// </summary>
        /// <param name="configuration">Configuration</param>
        /// <param name="connectionString">Connection string for Mongo database, for example 'mongodb://username:password@host:port'</param>
        /// <param name="databaseName">Name of database at Mongo server</param>
        /// <returns></returns>
        public static IGlobalConfiguration<MongoStorage> UseMongoStorage(this IGlobalConfiguration configuration,
            string connectionString,
            string databaseName)
        {
            return UseMongoStorage(configuration, connectionString, databaseName, new MongoStorageOptions());
        }

        /// <summary>
        /// Configure Hangfire to use MongoDB storage
        /// </summary>
        /// <param name="configuration">Configuration</param>
        /// <param name="connectionString">Connection string for Mongo database, for example 'mongodb://username:password@host:port/database'</param>
        /// <param name="storageOptions">Storage options</param>
        /// <exception cref="ArgumentException">Thrown if the connection string does not include the database name</exception>
        /// <returns></returns>
        public static IGlobalConfiguration<MongoStorage> UseMongoStorage(this IGlobalConfiguration configuration,
            string connectionString,
            MongoStorageOptions storageOptions)
        {
            var mongoUrlBuilder = new MongoUrlBuilder(connectionString);
            var databaseName = mongoUrlBuilder.DatabaseName;
            if (string.IsNullOrEmpty(databaseName))
            {
                throw new ArgumentException("The connection string must include the database name, see https://docs.mongodb.com/manual/reference/connection-string/", nameof(connectionString));
            }
            var mongoClientSettings = MongoClientSettings.FromConnectionString(connectionString);
            return UseMongoStorage(configuration, mongoClientSettings, databaseName, storageOptions);
        }

        /// <summary>
        /// Configure Hangfire to use MongoDB storage
        /// </summary>
        /// <param name="configuration">Configuration</param>
        /// <param name="connectionString">Connection string for Mongo database, for example 'mongodb://username:password@host:port'</param>
        /// <param name="databaseName">Name of database at Mongo server</param>
        /// <param name="storageOptions">Storage options</param>
        /// <returns></returns>
        public static IGlobalConfiguration<MongoStorage> UseMongoStorage(this IGlobalConfiguration configuration,
            string connectionString,
            string databaseName,
            MongoStorageOptions storageOptions)
        {
            return UseMongoStorage(configuration, MongoClientSettings.FromConnectionString(connectionString),
                databaseName, storageOptions);
        }

        /// <summary>
        /// Configure Hangfire to use MongoDB storage
        /// </summary>
        /// <param name="configuration">Configuration</param>
        /// <param name="mongoClientSettings">Client settings for Mongo</param>
        /// <param name="databaseName">Name of database at Mongo server</param>
        /// <returns></returns>
        public static IGlobalConfiguration<MongoStorage> UseMongoStorage(this IGlobalConfiguration configuration,
            MongoClientSettings mongoClientSettings,
            string databaseName)
        {
            return UseMongoStorage(configuration, mongoClientSettings, databaseName, new MongoStorageOptions());
        }

        /// <summary>
        /// Configure Hangfire to use MongoDB storage
        /// </summary>
        /// <param name="configuration">Configuration</param>
        /// <param name="mongoClientSettings">Client settings for Mongo</param>
        /// <param name="databaseName">Name of database at Mongo server</param>
        /// <param name="storageOptions">Storage options</param>
        /// <returns></returns>
        public static IGlobalConfiguration<MongoStorage> UseMongoStorage(this IGlobalConfiguration configuration,
            MongoClientSettings mongoClientSettings,
            string databaseName,
            MongoStorageOptions storageOptions)
        {
            var storage = new MongoStorage(mongoClientSettings, databaseName, storageOptions);

            return configuration.UseStorage(storage);
        }

        /// <summary>
        /// Configure Hangfire to use MongoDB storage
        /// </summary>
        /// <param name="configuration">Configuration</param>
        /// <param name="mongoClient">Client for Mongo</param>
        /// <param name="databaseName">Name of database at Mongo server</param>
        /// <param name="storageOptions">Storage options</param>
        /// <returns></returns>
        public static IGlobalConfiguration<MongoStorage> UseMongoStorage(this IGlobalConfiguration configuration,
            IMongoClient mongoClient,
            string databaseName,
            MongoStorageOptions storageOptions)
        {
            var storage = new MongoStorage(mongoClient, databaseName, storageOptions);

            return configuration.UseStorage(storage);
        }
    }
}