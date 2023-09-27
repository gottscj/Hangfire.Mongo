using Hangfire.Logging;
using MongoDB.Bson;
using MongoDB.Driver;
using System;

namespace Hangfire.Mongo.Database
{
    /// <summary>
    /// Represents Mongo database context for Hangfire
    /// </summary>
    public sealed class HangfireDbContext
    {
        private static readonly ILog Logger = LogProvider.For<HangfireDbContext>();

        private readonly string _prefix;

        /// <summary>
        /// MongoClient used for this db context instance
        /// </summary>
        public IMongoClient Client { get; }

        /// <summary>
        /// Database instance used for this db context instance
        /// </summary>
        public IMongoDatabase Database { get; }

        /// <summary>
        /// Needed to check for features enabled only in certain versions of the database.
        /// </summary>
        private Version _version = new Version(0, 0);

        internal HangfireDbContext(string connectionString, string databaseName, string prefix = "hangfire")
            : this(new MongoClient(connectionString), databaseName, prefix)
        {
        }

        /// <summary>
        /// Constructs context with Mongo client and database name
        /// </summary>
        /// <param name="mongoClient"></param>
        /// <param name="databaseName"></param>
        /// <param name="prefix"></param>
        public HangfireDbContext(IMongoClient mongoClient, string databaseName, string prefix = "hangfire")
        {
            _prefix = prefix;
            Client = mongoClient;
            Database = mongoClient.GetDatabase(databaseName);
            ConnectionId = Guid.NewGuid().ToString();

            GetServerVersion();
        }

        /// <summary>
        /// Try to get server version, sometimes it can be useful to use certain feature only
        /// if the version is greater than a minimum version.
        /// </summary>
        private void GetServerVersion()
        {
            //We cache version in memory, it is unlikely that the database changes version while the application is running and
            //if we want to use newer feature it is a good idea to restart.
            if (_version != null)
            {
                return;
            }
            try
            {
                //We need to get server version to check if we can user various features.
                var adminDatabase = Client.GetDatabase("admin");

                // Check the server version
                var buildInfoCommand = new BsonDocumentCommand<BsonDocument>(new BsonDocument { { "buildInfo", 1 } });
                var buildInfo = adminDatabase.RunCommand(buildInfoCommand);
                var versionString = buildInfo.GetValue("version").AsString;
                _version = Version.Parse(versionString);
                if (Logger.IsInfoEnabled())
                {
                    Logger.InfoFormat("Mongodb version is {0}", _version);
                }
            }
            catch (Exception e)
            {
                //we are unable to determine MongoDB Server version, this is a warning, now knowing a version will not cause
                //any issues. Returning a version of 0 will ensure no new feature are used unless we are sure they are supported.
                _version = new Version(0, 0);
                Logger.WarnException("Failed to get Mongodb server version", e);
            }
        }

        /// <summary>
        /// Returns true if the version if greater than or equal of required version.
        /// </summary>
        /// <param name="major"></param>
        /// <param name="minor"></param>
        /// <returns>True if the version greater than or equal to requested version.</returns>
        public bool IsVersionGreaterThanOrEqualTo(int major, int minor)
        {
            return _version.Major > major || (_version.Major == major && _version.Minor >= minor);
        }

        /// <summary>
        /// Mongo database connection identifier
        /// </summary>
        public string ConnectionId { get; private set; }

        /// <summary>
        /// Reference to tailable collection which contains signal dtos for enqueued job items
        /// </summary>
        public IMongoCollection<BsonDocument> Notifications =>
            Database.GetCollection<BsonDocument>(_prefix + ".notifications");

        /// <summary>
        /// Reference to job graph collection
        /// </summary>
        public IMongoCollection<BsonDocument> JobGraph => Database.GetCollection<BsonDocument>(_prefix + ".jobGraph");

        /// <summary>
        /// Reference to collection which contains distributed locks
        /// </summary>
        public IMongoCollection<BsonDocument> DistributedLock => Database
            .GetCollection<BsonDocument>(_prefix + ".locks");

        /// <summary>
        /// Reference to collection which contains schemas
        /// </summary>
        public IMongoCollection<BsonDocument> Schema => Database.GetCollection<BsonDocument>(_prefix + ".schema");

        /// <summary>
        /// Reference to collection which contains servers information
        /// </summary>
        public IMongoCollection<BsonDocument> Server => Database.GetCollection<BsonDocument>(_prefix + ".server");
    }
}