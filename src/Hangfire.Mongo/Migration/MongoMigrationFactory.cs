using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Hangfire.Mongo.Migration.Steps;

namespace Hangfire.Mongo.Migration
{
    /// <summary>
    /// Creates migration steps
    /// </summary>
    public class MongoMigrationFactory
    {
        /// <summary>
        /// Returns migration steps by schema, then by sequence 
        /// </summary>
        /// <returns></returns>
        public virtual IList<IMongoMigrationStep> GetOrderedMigrations()
        {
            var types = typeof(IMongoMigrationStep).GetTypeInfo().Assembly.GetTypes();
            return types
                .Where(t => !t.GetTypeInfo().IsAbstract && t.GetTypeInfo().GetInterfaces().Contains(typeof(IMongoMigrationStep)))
                .Select(t => (IMongoMigrationStep)Activator.CreateInstance(t))
                .OrderBy(step => (int)step.TargetSchema)
                .ThenBy(step => step.Sequence)
                .ToList();
        }
    }
}