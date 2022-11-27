using System;
using System.Diagnostics;
using System.Linq;
using Hangfire.Logging;
using Hangfire.Mongo.Dto;
using MongoDB.Bson;
using MongoDB.Driver;
#pragma warning disable 1591

namespace Hangfire.Mongo.Migration.Strategies
{
    public abstract class MongoMigrationStrategy
    {
        private readonly IMongoMigrationContext _mongoMigrationContext;
        private static readonly ILog Logger = LogProvider.For<MongoMigrationStrategy>();
        protected MongoStorageOptions StorageOptions;

        protected MongoMigrationStrategy(IMongoMigrationContext mongoMigrationContext)
        {
            _mongoMigrationContext = mongoMigrationContext;
        }
        
        public void Execute(MongoStorageOptions storageOptions, IMongoDatabase database, MongoSchema fromSchema, MongoSchema toSchema)
        {
            StorageOptions = storageOptions;
            ExecuteStrategy(database, fromSchema, toSchema);
            ExecuteMigration(database, fromSchema, toSchema);
        }

        protected virtual void ExecuteMigration(IMongoDatabase database, MongoSchema fromSchema, MongoSchema toSchema)
        {
            if (fromSchema == toSchema)
            {
                // Nothing to migrate - let's just get outa here
                return;
            }

            if (fromSchema > toSchema)
            {
                throw new InvalidOperationException($@"The {nameof(fromSchema)} ({fromSchema}) cannot be larger than {nameof(toSchema)} ({toSchema})");
            }

            var migrationSteps = StorageOptions
                .MigrationOptions
                .MongoMigrationFactory
                .GetOrderedMigrations()
                .Where(step => step.TargetSchema > fromSchema && step.TargetSchema <= toSchema)
                .GroupBy(step => step.TargetSchema);

            var migrationSw = Stopwatch.StartNew();
            var schemas = database.GetCollection<BsonDocument>(StorageOptions.Prefix + ".schema");
            foreach (var migrationGroup in migrationSteps)
            {
                Logger.Info(() =>
                    $"Executing migration for schema '{migrationGroup.Key}'");
                foreach (var migrationStep in migrationGroup)
                {
                    try
                    {
                        var sw = Stopwatch.StartNew();
                        if (!migrationStep.Execute(database, StorageOptions, _mongoMigrationContext))
                        {
                            throw new MongoMigrationException(migrationStep);
                        }

                        Logger.Info(() =>
                            $"Executed migration step: {migrationStep.GetType().Name}[{migrationStep.Sequence}] in {sw.ElapsedMilliseconds}ms");
                    }
                    catch (MongoMigrationException)
                    {
                        throw;
                    }
                    catch (Exception e)
                    {
                        throw new MongoMigrationException(migrationStep, e);
                    }
                }

                // We just completed a migration to the next schema.
                // Update the schema info and continue.
                
                var schemaDoc = schemas.FindOneAndDelete(new BsonDocument());
                var schemaDto = new SchemaDto(schemaDoc);
                schemaDto.Version = migrationGroup.Key;
                schemas.InsertOne(schemaDto.Serialize());   
            }
            
            Logger.Info(() =>
                $"Instance with clientId: {StorageOptions.ClientId} is executed migration from {fromSchema} -> {toSchema} in {migrationSw.ElapsedMilliseconds}ms");
        }
        
        protected abstract void ExecuteStrategy(IMongoDatabase database, MongoSchema fromSchema, MongoSchema toSchema);
        
    }
}