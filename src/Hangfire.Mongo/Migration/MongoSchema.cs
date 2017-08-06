using System;
using System.Collections.Generic;
using System.Linq;

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

        /// <summary>
        /// Is the schema version the last and current one.
        /// </summary>
        internal static bool IsLast(this MongoSchema schema)
        {
            return Enum.GetValues(typeof(MongoSchema))
                       .OfType<MongoSchema>()
                       .Last() == schema;
        }


        /// <summary>
        /// Get the next schema version.
        /// </summary>
        /// <returns>The next.</returns>
        /// <param name="schema">Schema.</param>
        internal static MongoSchema Next(this MongoSchema schema)
        {
            var schemaValues = Enum.GetValues(typeof(MongoSchema))
                                   .OfType<MongoSchema>()
                                   .ToList();
            return schemaValues.ElementAt(schemaValues.IndexOf(schema));
        }


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