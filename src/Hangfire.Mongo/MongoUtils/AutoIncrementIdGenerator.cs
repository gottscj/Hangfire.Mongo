using System;
using System.Reflection;
using Hangfire.Mongo.Dto;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;

namespace Hangfire.Mongo.MongoUtils
{
    /// <summary>
    /// Represents ID generator for Mongo database
    /// </summary>
    public abstract class AutoIncrementIdGenerator : IIdGenerator
    {
        private readonly string _prefix;

        /// <summary>
        /// Constructs ID generator with empty prefix
        /// </summary>
        protected AutoIncrementIdGenerator()
            : this(String.Empty)
        {
        }

        /// <summary>
        /// Constructs ID generator with specified prefix
        /// </summary>
        /// <param name="prefix">Collection name prefix</param>
        protected AutoIncrementIdGenerator(string prefix)
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
            Type containerType = container.GetType();
            PropertyInfo databaseProperty = containerType.GetProperty("Database");
            PropertyInfo namespaceProperty = containerType.GetProperty("CollectionNamespace");

            if (databaseProperty == null)
                throw new InvalidOperationException("Unable to locate \"Database\" property.");

            if (namespaceProperty == null)
                throw new InvalidOperationException("Unable to locate \"CollectionNamespace\" property.");

            IMongoDatabase database = databaseProperty.GetValue(container) as IMongoDatabase;
            CollectionNamespace collectionNamespace = namespaceProperty.GetValue(container) as CollectionNamespace;

            if (database == null)
                throw new InvalidOperationException("Database reference is null.");

            if (collectionNamespace == null)
                throw new InvalidOperationException("CollectionNamespace reference is null.");

            var idSequenceCollection = database.GetCollection<IdentifierDto>(_prefix + "_identifiers");

            IdentifierDto result = idSequenceCollection.FindOneAndUpdate(
                Builders<IdentifierDto>.Filter.Eq(_ => _.Id, collectionNamespace.CollectionName),
                Builders<IdentifierDto>.Update.Inc(_ => _.Seq, 1),
                new FindOneAndUpdateOptions<IdentifierDto>()
                {
                    IsUpsert = true,
                    ReturnDocument = ReturnDocument.After
                });

            return FormatNumber(result.Seq);
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

        /// <summary>
        /// Converts sequence number into appropriate format
        /// </summary>
        /// <param name="input">Number</param>
        /// <returns>Converted number</returns>
        protected abstract object FormatNumber(long input);
    }
}