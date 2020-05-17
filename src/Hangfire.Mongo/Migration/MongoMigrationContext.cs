using System.Collections.Generic;

namespace Hangfire.Mongo.Migration
{
    /// <summary>
    /// Context used to store state between migrations
    /// </summary>
    public interface IMongoMigrationContext
    {
        /// <summary>
        /// Gets item from context environment
        /// </summary>
        /// <param name="key"></param>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        T GetItem<T>(string key);

        /// <summary>
        /// Sets item in migration context environment
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <typeparam name="T"></typeparam>
        void SetItem<T>(string key, T value);
    }


    /// <inheritdoc />
    public class MongoMigrationContext : IMongoMigrationContext
    {
        private readonly Dictionary<string, object> _environment = new Dictionary<string, object>();

        /// <inheritdoc />
        public T GetItem<T>(string key)
        {
            return (T)_environment[key];
        }

        /// <inheritdoc />
        public void SetItem<T>(string key, T value)
        {
            _environment[key] = value;
        }
    }
}