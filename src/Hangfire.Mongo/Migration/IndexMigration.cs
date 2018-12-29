using System;
using System.Collections.Generic;
using System.Linq;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Hangfire.Mongo.Migration
{
    internal abstract class IndexMigration
    {
        /// <summary>
        /// Create indexes with <paramref name="indexNames"/> in the <paramref name="collection"/>.
        /// If an index already exists, and it is the same, it will be overwritten.
        /// If the index is different to an existing index, an exception will be thrown.
        /// </summary>
        /// <param name="collection">
        /// The collection to create indexes in.
        /// </param>
        /// <param name="indexType">
        /// The type of the indexes created.
        /// </param>
        /// <param name="indexNames">
        /// The names of the indexes to create.
        /// </param>
        /// <returns>
        /// An <see cref="IEnumerable{T}"/> of the names of the indexes that were created.
        /// </returns>
        /// <exception cref="MongoCommandException">
        /// Thrown if an existing index is attempted changed.
        /// </exception>
        /// <example>
        /// <code>
        /// collection.TryCreateIndexes(Builders{BsonDocument}.IndexKeys.Descending, "Name")
        /// </code>
        /// </example>
        protected void TryCreateIndexes(IMongoCollection<BsonDocument> collection, Func<FieldDefinition<BsonDocument>, IndexKeysDefinition<BsonDocument>> indexType, params string[] indexNames)
        {
            if (collection == null)
            {
                throw new ArgumentNullException(nameof(collection));
            }
            if (indexType == null)
            {
                throw new ArgumentNullException(nameof(indexType));
            }
            if (indexNames == null)
            {
                throw new ArgumentNullException(nameof(indexNames));
            }
            if (indexNames.Length == 0)
            {
                throw new ArgumentException("Must have at least one index name", nameof(indexNames));
            }

            DropExistingIndexes(collection, indexNames);
            CreateIndexes(collection, indexType, indexNames, indexName => new CreateIndexOptions
            {
                Name = indexName,
                Sparse = true
            });
        }

        protected void TryCreateUniqueIndexes(IMongoCollection<BsonDocument> collection,
            Func<FieldDefinition<BsonDocument>, IndexKeysDefinition<BsonDocument>> indexType,
            params string[] indexNames)
        {
            if (collection == null)
            {
                throw new ArgumentNullException(nameof(collection));
            }
            if (indexType == null)
            {
                throw new ArgumentNullException(nameof(indexType));
            }
            if (indexNames == null)
            {
                throw new ArgumentNullException(nameof(indexNames));
            }
            if (indexNames.Length == 0)
            {
                throw new ArgumentException("Must have at least one index name", nameof(indexNames));
            }

            DropExistingIndexes(collection, indexNames);
            CreateIndexes(collection, indexType, indexNames, indexName => new CreateIndexOptions
            {
                Name = indexName,
                Sparse = true,
                Unique = true
            });
        }

        private static void CreateIndexes(IMongoCollection<BsonDocument> collection,
            Func<FieldDefinition<BsonDocument>, IndexKeysDefinition<BsonDocument>> getIndexType, string[] indexNames,
            Func<string, CreateIndexOptions> createOptions)
        {
            var indexModels = indexNames.Select(indexName =>
            {
                var index = getIndexType(indexName);
                return new CreateIndexModel<BsonDocument>(index, createOptions(indexName));
            }).ToList();

            collection.Indexes.CreateMany(indexModels);
        }

        private static void DropExistingIndexes(IMongoCollection<BsonDocument> collection, string[] indexNames)
        {
            // drop existing indexes for the 'Resource' field if any exist
            using (var cursor = collection.Indexes.List())
            {
                var existingResourceIndexes = cursor.ToList();
                foreach (var index in existingResourceIndexes)
                {
                    var indexName = index["name"].AsString;
                    if (indexNames.Contains(indexName))
                    {
                        collection.Indexes.DropOne(indexName);
                    }
                }
            }
        }
    }
}