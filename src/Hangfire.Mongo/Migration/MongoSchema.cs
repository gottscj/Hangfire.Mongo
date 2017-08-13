using System;
using System.Collections.Generic;

namespace Hangfire.Mongo.Migration
{

    /// <summary>
    /// All the hangfire mongo schema versions ever used
    /// </summary>
    internal enum MongoSchema
    {
        None = 0,
        Version4 = 4,
        Version5 = 5,
        Version6 = 6,
        Version7 = 7,
    }


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

                case MongoSchema.Version4:
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

                case MongoSchema.Version5:
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

                case MongoSchema.Version6:
                    return new[] {
                        prefix + ".job",
                        prefix + ".jobQueue",
                        prefix + ".locks",
                        prefix + ".schema",
                        prefix + ".server",
                        prefix + ".statedata"
                    };

                case MongoSchema.Version7:
                    return new[] {
                        prefix + ".job",
                        prefix + ".jobQueue",
                        prefix + ".locks",
                        prefix + ".schema",
                        prefix + ".server",
                        prefix + ".stateData"
                    };

                default:
                    throw new ArgumentException($@"Unknown schema: '{schema}'", nameof(schema));
            }
        }


    }
}