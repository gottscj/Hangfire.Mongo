using System;
using System.Linq.Expressions;
using Hangfire.Mongo.Database;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Hangfire.Mongo.MongoUtils
{
    /// <summary>
    /// Helper utilities to work with Mongo database
    /// </summary>
    public static class MongoExtensions
    {
        /// <summary>
        /// Retreives server time in UTC zone
        /// </summary>
        /// <param name="database">Mongo database</param>
        /// <returns>Server time</returns>
        public static DateTime GetServerTimeUtc(this IMongoDatabase database)
        {
            var serverStatus = database.RunCommand<BsonDocument>(new BsonDocument("isMaster", 1));
            BsonValue localTime;
            return serverStatus.TryGetValue("localTime", out localTime)
                ? ((DateTime)localTime).ToUniversalTime()
                : DateTime.UtcNow;
        }

        /// <summary>
        /// Retreives server time in UTC zone
        /// </summary>
        /// <param name="dbContext">Hangfire database context</param>
        /// <returns>Server time</returns>
        public static DateTime GetServerTimeUtc(this HangfireDbContext dbContext)
        {
            return GetServerTimeUtc(dbContext.Database);
        }


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
            var exp = field.Body as UnaryExpression;
            var memberExp = exp?.Operand as MemberExpression;
            return memberExp?.Member.Name;
        }

    }
}