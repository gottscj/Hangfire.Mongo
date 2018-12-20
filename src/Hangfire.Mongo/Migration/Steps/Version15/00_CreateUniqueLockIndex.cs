using Hangfire.Mongo.Dto;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Hangfire.Mongo.Migration.Steps.Version14
{
    /// <summary>
    /// Drops the existing index for the 'Resource' field of the distributed
    /// lock collection and creates a new one that guarantees uniqueness.
    /// This prevents a race case in which multiple Hangfire servers can 
    /// acquire a lock on the same resource simultaneously.
    /// </summary>
    internal class CreateUniqueLockIndex : IMongoMigrationStep
    {   
        private readonly string _lockResourceIndexName = nameof(DistributedLockDto.Resource);

        public MongoSchema TargetSchema => MongoSchema.Version15;

        public long Sequence => 0;

        public bool Execute(IMongoDatabase database, MongoStorageOptions storageOptions, IMongoMigrationBag migrationBag)
        {
            var locksCollection = database.GetCollection<BsonDocument>($@"{storageOptions.Prefix}.locks");

            // drop existing indexes for the 'Resource' field if any exist
            using (var cursor = locksCollection.Indexes.List())
            {
                var existingResourceIndexes = cursor.ToList();
                foreach (var index in existingResourceIndexes)
                {
                    var indexName = index["name"].AsString;
                    if (indexName.Contains(_lockResourceIndexName))
                    {
                        locksCollection.Indexes.DropOne(indexName);
                    }
                }
            }

            // create new unique index for the 'Resource' field
            var indexOptions = new CreateIndexOptions
            {
                Name = _lockResourceIndexName,
                Sparse = true,
                Unique = true
            };
            var indexBuilder = Builders<BsonDocument>.IndexKeys;
            var indexModel = new CreateIndexModel<BsonDocument>(indexBuilder.Descending(_lockResourceIndexName), indexOptions);
            locksCollection.Indexes.CreateOne(indexModel);

            return true;
        }
    }
}