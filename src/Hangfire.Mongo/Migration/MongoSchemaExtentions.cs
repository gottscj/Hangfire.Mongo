using System;
using System.Collections.Generic;
using System.Linq;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Hangfire.Mongo.Migration
{

    /// <summary>
    /// Helpers for MongoSchema
    /// </summary>
    internal static class MongoSchemaExtentions
    {

        internal static IList<string> CollectionNames(this MongoSchema schema, string prefix)
        {
            switch (schema)
            {
                case MongoSchema.None:
                    throw new ArgumentException($@"The '{schema}' has no collections", nameof(schema));

                case MongoSchema.Version04:
                    return new[] {
                        "_identifiers", // A bug prevented the use of prefix
                        prefix + ".counter",
                        prefix + ".hash",
                        prefix + ".job",
                        prefix + ".jobParameter",
                        prefix + ".jobQueue",
                        prefix + ".list",
                        prefix + ".locks",
                        prefix + ".schema",
                        prefix + ".server",
                        prefix + ".set",
                        prefix + ".state",
                    };

                case MongoSchema.Version05:
                    return new[] {
                        "_identifiers", // A bug prevented the use of prefix
                        prefix + ".counter",
                        prefix + ".hash",
                        prefix + ".job",
                        prefix + ".jobParameter",
                        prefix + ".jobQueue",
                        prefix + ".list",
                        prefix + ".locks",
                        prefix + ".schema",
                        prefix + ".server",
                        prefix + ".set",
                        prefix + ".state",
                    };

                case MongoSchema.Version06:
                    return new[] {
                        prefix + ".job",
                        prefix + ".jobQueue",
                        prefix + ".locks",
                        prefix + ".schema",
                        prefix + ".server",
                        prefix + ".statedata"
                    };

                case MongoSchema.Version07:
                case MongoSchema.Version08:
                    return new[] {
                        prefix + ".job",
                        prefix + ".jobQueue",
                        prefix + ".locks",
                        prefix + ".schema",
                        prefix + ".server",
                        prefix + ".stateData"
                    };

                case MongoSchema.Version09:
                case MongoSchema.Version10:
                case MongoSchema.Version11:
                case MongoSchema.Version12:
                    return new[] {
                        prefix + ".job",
                        prefix + ".jobQueue",
                        prefix + ".locks",
                        prefix + ".schema",
                        prefix + ".server",
                        prefix + ".signal",
                        prefix + ".stateData"
                    };
                
                case MongoSchema.Version13:
                    return new[]
                    {
                        prefix + ".jobGraph",
                        prefix + ".locks",
                        prefix + ".schema",
                        prefix + ".server",
                        prefix + ".signal"
                    };

                default:
                    throw new ArgumentException($@"Unknown schema: '{schema}'", nameof(schema));
            }
        }


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
        /// An <see cref="IEnumerable{String}"/> of the names of the indexes that were created.
        /// </returns>
        /// <exception cref="MongoCommandException">
        /// Thrown if an existing index is attempted changed.
        /// </exception>
        /// <example>
        /// <code>
        /// collection.TryCreateIndexes(Builders{BsonDocument}.IndexKeys.Descending, "Name")
        /// </code>
        /// </example>
        internal static IEnumerable<string> TryCreateIndexes(this IMongoCollection<BsonDocument> collection, Func<FieldDefinition<BsonDocument>, IndexKeysDefinition<BsonDocument>> indexType, params string[] indexNames)
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

            var indexModels = indexNames.Select(indexName =>
            {
                var index = indexType(indexName);
                var options = new CreateIndexOptions
                {
                    Name = indexName,
                    Sparse = true
                };
                return new CreateIndexModel<BsonDocument>(index, options);
            }).ToList();

            return collection.Indexes.CreateMany(indexModels);
        }
    }
}