using System;
using System.Collections.Generic;
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
            dynamic serverStatus = database.RunCommand<dynamic>(new BsonDocument("isMaster", 1));
            object localTime;
            if (((IDictionary<string, object>)serverStatus).TryGetValue("localTime", out localTime))
            {
                return ((DateTime)localTime).ToUniversalTime();
            }
            return DateTime.UtcNow;
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
            var exp = field.Body as UnaryExpression;
            var memberExp = exp?.Operand as MemberExpression;
            var builder = new IndexKeysDefinitionBuilder<TDocument>();
            var options = new CreateIndexOptions<TDocument>
            {
                Name = name ?? memberExp?.Member.Name
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
            var exp = field.Body as UnaryExpression;
            var memberExp = exp?.Operand as MemberExpression;
            var builder = new IndexKeysDefinitionBuilder<TDocument>();
            var options = new CreateIndexOptions<TDocument>
            {
                Name = name ?? memberExp?.Member.Name
            };

            collection.Indexes.CreateOne(builder.Descending(field), options);
        }
    }
}