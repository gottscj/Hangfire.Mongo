using System;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;
using MongoDB.Driver.Builders;

namespace Hangfire.Mongo.MongoUtils
{
    /// <summary>
    /// Represents ID generator for Mongo database
    /// </summary>
    public class AutoIncrementIdGenerator : IIdGenerator
    {
        private readonly string _prefix;

        /// <summary>
        /// Constructs ID generator with empty prefix
        /// </summary>
        public AutoIncrementIdGenerator()
            : this(String.Empty)
        {
        }

        /// <summary>
        /// Constructs ID generator with specified prefix
        /// </summary>
        /// <param name="prefix">Collection name prefix</param>
        public AutoIncrementIdGenerator(string prefix)
        {
            _prefix = prefix ?? String.Empty;
        }

        /// <summary>
        /// Generates next ID from sequence
        /// </summary>
        /// <param name="container">Container which stores identifiers</param>
        /// <param name="document">Document which stores identifier</param>
        /// <returns>Generated identifier</returns>
        public object GenerateId(object container, object document)
        {
            var idSequenceCollection = ((MongoCollection)container).Database
                .GetCollection(_prefix + "_identifiers");

            var query = Query.EQ("_id", ((MongoCollection)container).Name);

            return (idSequenceCollection.FindAndModify(new FindAndModifyArgs
            {
                Query = query,
                Update = Update.Inc("seq", 1),
                VersionReturned = FindAndModifyDocumentVersion.Modified,
                Upsert = true
            }).ModifiedDocument["seq"]).AsInt32;
        }

        /// <summary>
        /// Checks wheter specified identifier is empty
        /// </summary>
        /// <param name="id">Identifier</param>
        /// <returns>True if identifier is empty; false otherwise</returns>
        public bool IsEmpty(object id)
        {
            return (int)id == 0;
        }
    }
}