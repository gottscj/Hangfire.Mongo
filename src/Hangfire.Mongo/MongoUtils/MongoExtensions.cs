using System;
using System.Linq.Expressions;
using MongoDB.Driver;

namespace Hangfire.Mongo.MongoUtils
{
    /// <summary>
    /// Helper utilities to work with Mongo database
    /// </summary>
    public static class MongoExtensions
    {
        /// <summary>
        /// Adds a ascending index on the field to the collection
        /// </summary>
        /// <param name="collection">The collection to add the index to</param>
        /// <param name="field">The field to add ascending index for</param>
        /// <param name="name">Name of the index. Can be null, then name is auto generated</param>
        /// <typeparam name="TDocument"></typeparam>
        public static void CreateAscendingIndex<TDocument>(this IMongoCollection<TDocument> collection, Expression<Func<TDocument, object>> field, string name = null)
        {
            var builder = new IndexKeysDefinitionBuilder<TDocument>();
            var options = new CreateIndexOptions<TDocument>
            {
                Name = name ?? field.GetFieldName()
            };
            collection.Indexes.CreateOne(builder.Ascending(field), options);
        }


        /// <summary>
        /// Adds a descending index on the field to the collection
        /// </summary>
        /// <param name="collection">The collection to add the index to</param>
        /// <param name="field">The field to add descending index for</param>
        /// <param name="name">Name of the index. Can be null, then name is auto generated</param>
        /// <typeparam name="TDocument"></typeparam>
        public static void CreateDescendingIndex<TDocument>(this IMongoCollection<TDocument> collection, Expression<Func<TDocument, object>> field, string name = null)
        {
            var builder = new IndexKeysDefinitionBuilder<TDocument>();
            var options = new CreateIndexOptions<TDocument>
            {
                Name = name ?? field.GetFieldName()
            };
            collection.Indexes.CreateOne(builder.Descending(field), options);
        }


        /// <summary>
        /// Try to extract the field name from the expression.
        /// </summary>
        /// <typeparam name="TDocument"></typeparam>
        /// <param name="field">
        /// The expression to extract from.
        /// </param>
        /// <returns>
        /// On success the field name, else null
        /// </returns>
        private static string GetFieldName<TDocument>(this Expression<Func<TDocument, object>> field)
        {
            var body = field.Body as MemberExpression;

            if (body != null) return body.Member.Name;

            var ubody = (UnaryExpression)field.Body;
            body = ubody.Operand as MemberExpression;

            return body.Member.Name;
        }

    }
}