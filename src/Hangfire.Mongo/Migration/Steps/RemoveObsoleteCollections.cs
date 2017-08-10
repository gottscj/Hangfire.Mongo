using System;
using System.Collections.Generic;
using System.Linq;
using MongoDB.Driver;

namespace Hangfire.Mongo.Migration.Steps
{
    /// <summary>
    /// Removes obsolete collections from previous schema to current
    /// </summary>
    internal abstract class RemoveObsoleteCollectionsStep : IMongoMigrationStep
    {

        public abstract MongoSchema TargetSchema { get; }

        public abstract long Sequence { get; }

        public virtual bool Execute(IMongoDatabase database, MongoStorageOptions storageOptions, IMongoMigrationBag migrationBag)
        {
            foreach (var previousCollectionName in ObsoleteCollectionNames(database, storageOptions))
            {
                database.DropCollection(previousCollectionName);
            }

            return true;
        }


        protected IEnumerable<string> ObsoleteCollectionNames(IMongoDatabase database, MongoStorageOptions storageOptions)
        {
            var mongoSchemas = Enum.GetValues(typeof(MongoSchema)).Cast<MongoSchema>().OrderBy(v => v).ToList();
            var index = mongoSchemas.IndexOf(TargetSchema);
            if (index <= 0)
            {
                return Enumerable.Empty<string>();
            }
            var previousCollectionNames = mongoSchemas[index - 1].CollectionNames(storageOptions.Prefix);
            var collectionNames = TargetSchema.CollectionNames(storageOptions.Prefix);
            return previousCollectionNames.Where(name => !collectionNames.Contains(name));
        }
    }
}
