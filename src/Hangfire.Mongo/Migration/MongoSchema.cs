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
        Version5 = 5,
        Version6 = 6,
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

                case MongoSchema.Version5:
                    return new[] {
                        prefix + ".statedata",
                        prefix + ".locks",
                        prefix + ".job",
                        prefix + ".jobQueue",
                        prefix + ".schema",
                        prefix + ".server"
                    };

                case MongoSchema.Version6:
                    return new[] {
                        prefix + ".stateData",
                        prefix + ".locks",
                        prefix + ".job",
                        prefix + ".jobQueue",
                        prefix + ".schema",
                        prefix + ".server"
                    };

                default:
                    throw new ArgumentException($@"Unknown schema: '{schema}'", nameof(schema));
            }
        }


    }
}