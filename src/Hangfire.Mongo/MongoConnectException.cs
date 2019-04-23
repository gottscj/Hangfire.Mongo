using System;
using Hangfire.Mongo.Database;

namespace Hangfire.Mongo
{
    /// <summary>
    /// Thrown if Hangfire.Mongo is unable to connect to the database
    /// </summary>
    public class MongoConnectException : Exception
    {
        /// <summary>
        /// ctor
        /// </summary>
        /// <param name="dbContext"></param>
        /// <param name="connectionString"></param>
        /// <param name="e"></param>
        public MongoConnectException(HangfireDbContext dbContext, string connectionString, Exception e) 
            : base($"\r\nDid not receive ping response from '{dbContext.Database.DatabaseNamespace.DatabaseName}' within 1000ms\r\n" +
                   $"assuming not able to connect to '{connectionString}'\r\n" +
                   $"you can disable database ping via the MongoStorageOption 'CheckConnection' field\r\n", e)
        {
            
        }
    }
}