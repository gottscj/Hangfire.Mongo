using System;

namespace Hangfire.Mongo.DistributedLock
{
    /// <summary>
    /// Represents exceptions for distributed lock implementation for MongoDB
    /// </summary>
#if !NETSTANDARD1_5
    [Serializable]
#endif
    public class MongoDistributedLockException : Exception
    {
        /// <summary>
        /// Creates exception
        /// </summary>
        /// <param name="message">Exception message</param>
        public MongoDistributedLockException(string message)
            : base(message)
        {
        }

        /// <summary>
        /// Creates exception with inner exception
        /// </summary>
        /// <param name="message">Exception message</param>
        /// <param name="innerException">Inner exception</param>
        public MongoDistributedLockException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}