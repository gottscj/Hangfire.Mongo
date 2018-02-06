using MongoDB.Bson;
using MongoDB.Driver;
using System.Linq;

namespace Hangfire.Mongo.Migration.Steps.Version11
{
    /// <summary>
    /// Automatically create indexes
    /// </summary>
    internal class CreateIndexes : IMongoMigrationStep
    {
        /// <summary>
        /// The schema this migration step targets.
        /// </summary>
        public MongoSchema TargetSchema => MongoSchema.Version11;

        /// <summary>
        /// Specifies the order migration steps for the same schema is executed.
        /// </summary>
        public long Sequence => 1;

        /// <summary>
        /// Executes the migration step.
        /// </summary>
        /// <param name="database">The mongo database.</param>
        /// <param name="storageOptions">Storage options.</param>
        /// <param name="migrationBag">Bag for storing data between migration steps</param>
        /// <returns>
        /// True on success, else false
        /// </returns>
        public bool Execute(IMongoDatabase database, MongoStorageOptions storageOptions, IMongoMigrationBag migrationBag)
        {
            var _prefix = storageOptions.Prefix;

            TryCreateIndexes(database.GetCollection<BsonDocument>(_prefix + ".jobQueue"), "JobId", "Queue", "FetchedAt");
            TryCreateIndexes(database.GetCollection<BsonDocument>(_prefix + ".job"), "StateName", "ExpireAt");
            TryCreateIndexes(database.GetCollection<BsonDocument>(_prefix + ".stateData"), "Key", "ExpireAt", "_t");
            TryCreateIndexes(database.GetCollection<BsonDocument>(_prefix + ".locks"), "Resource", "ExpireAt");
            TryCreateIndexes(database.GetCollection<BsonDocument>(_prefix + ".server"), "LastHeartbeat");
            TryCreateIndexes(database.GetCollection<BsonDocument>(_prefix + ".signal"), "Signaled");

            return true;
        }

        /// <summary>
        /// Tries the create indexes.
        /// </summary>
        /// <param name="collection">The collection.</param>
        /// <param name="names">The names.</param>
        private void TryCreateIndexes(IMongoCollection<BsonDocument> collection, params string[] names)
        {
            var list = collection.Indexes.List().ToList();
            var exist_indexes = list.Select(o => o["name"].AsString).ToList();
            foreach (var name in names)
            {
                if (exist_indexes.Any(v => v.Contains(name)))
                    continue;

                var index = new BsonDocumentIndexKeysDefinition<BsonDocument>(new BsonDocument(name, -1));
                var options = new CreateIndexOptions
                {
                    Name = name,
                    Sparse = true,
                };
                collection.Indexes.CreateOne(index, options);
            }
        }
    }
}