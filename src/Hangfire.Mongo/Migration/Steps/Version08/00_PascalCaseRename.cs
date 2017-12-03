using System;
using System.Collections.Generic;
using System.Linq;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Hangfire.Mongo.Migration.Steps.Version08
{
    /// <summary>
    /// Renames all properties in Hangfire collections to use PascalCase.
    /// This is just in case Hangfire.Mong has been used with e.g. camelCase.
    /// Migrating of snake_case, kebab-case or other casing that introduces
    /// extra characters is not supported.
    /// </summary>
    internal class PascalCaseRename : IMongoMigrationStep
    {

        /// <summary>
        /// A list of all property names used in schema version 8.
        /// </summary>
        private readonly List<string> _propertyNames = new List<string>
        {
            "Resource",
            "CreatedAt",
            "ExpireAt",
            "FetchedAt",
            "StartedAt",
            "Field",
            "InvocationData",
            "Arguments",
            "Parameters",
            "StateId",
            "StateName",
            "StateReason",
            "StateData",
            "StateHistory",
            "JobId",
            "Queue",
            "Key",
            "Value",
            "Version",
            "WorkerCount",
            "Queues",
            "Data",
            "LastHeartbeat",
            "Score",
            "Name",
            "Reason",
            "ClientId",
            "Heartbeat",
            "LockCount"
        };

        public MongoSchema TargetSchema => MongoSchema.Version08;

        public long Sequence => 0;

        public bool Execute(IMongoDatabase database, MongoStorageOptions storageOptions, IMongoMigrationBag migrationBag)
        {
            foreach (var collectionName in TargetSchema.CollectionNames(storageOptions.Prefix))
            {
                FixCollection(database, collectionName);
            }

            return true;
        }

        private void FixCollection(IMongoDatabase database, string collectionName)
        {
            var filterBuilder = Builders<BsonDocument>.Filter;
            var updateBuilder = Builders<BsonDocument>.Update;
            var collection = database.GetCollection<BsonDocument>(collectionName);
            var documents = collection.Find(filterBuilder.Empty).ToList();

            // First we handle basic properties
            var mismatchedNames = documents
                .SelectMany(d =>
                    d.Names.Where(n => n != "_id" && n != "_t" && !_propertyNames.Contains(n)))
                .Distinct();
            var updates = mismatchedNames
                .Select(n =>
                    updateBuilder.Rename(n, _propertyNames.First(pn => pn.Equals(n, StringComparison.OrdinalIgnoreCase))))
                .ToList();
            if (updates.Any())
            {
                collection.UpdateMany(filterBuilder.Empty, updateBuilder.Combine(updates));
            }

            // Handle array properties (it is not possible to rename fields in an array)
            // https://docs.mongodb.com/manual/reference/operator/update/rename/#up._S_rename
            documents = collection.Find(filterBuilder.Empty).ToList();
            foreach (var document in documents)
            {
                // Find array properties eligible for update
                var arrayProperties = document.Where(e => e.Value.IsBsonArray)
                    .GroupBy(e => e, array =>
                        array.Value.AsBsonArray.OfType<BsonDocument>().SelectMany(d =>
                            d.Where(e => e.Name != "_id" && e.Name != "_t" && !_propertyNames.Contains(e.Name))
                                .Select(e => new { Document = d, Element = e })))
                    .Where(g => g.Any(e => e.Any()));

                // Update property names and generate update definitions
                var arrayUpdates = arrayProperties
                    .Select(group =>
                    {
                        foreach (var item in group.SelectMany(g => g).ToList())
                        {
                            var newName = _propertyNames.First(pn => pn.Equals(item.Element.Name, StringComparison.OrdinalIgnoreCase));
                            item.Document.Remove(item.Element.Name);
                            item.Document.Add(newName, item.Element.Value);
                        }
                        return updateBuilder.Set(group.Key.Name, group.Key.Value);
                    })
                    .ToList();

                if (arrayUpdates.Any())
                {
                    // Execute the update for array properties
                    collection.UpdateOne(filterBuilder.Eq("_id", document["_id"]), updateBuilder.Combine(arrayUpdates));
                }
            }
        }
    }



}
