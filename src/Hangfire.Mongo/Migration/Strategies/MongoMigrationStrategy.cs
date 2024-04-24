using System;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
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

        /// <summary>
        /// Validates required schema against current.
        /// </summary>
        /// <param name="requiredSchema"></param>
        /// <param name="currentSchema"></param>
        /// <exception cref="InvalidOperationException"></exception>
        public virtual void ValidateSchema(MongoSchema requiredSchema, MongoSchema currentSchema)
        {
            if (requiredSchema < currentSchema)
            {
                var assemblyName = GetType().GetTypeInfo().Assembly.GetName();
                throw new InvalidOperationException(
                    $"{Environment.NewLine}{assemblyName.Name} version: {assemblyName.Version}, uses a schema prior to the current database." +
                    $"{Environment.NewLine}Backwards migration is not supported. Please resolve this manually (e.g. by dropping the database)." +
                    $"{Environment.NewLine}You can also use the DropMongoMigrationStrategy which will drop all collections and migrate from anew" +
                    $"{Environment.NewLine}Please see https://github.com/sergeyzwezdin/Hangfire.Mongo#migration for further information.");
            }
        }

        public void Execute(MongoStorageOptions storageOptions, IMongoDatabase database, MongoSchema fromSchema,
            MongoSchema toSchema)
        {
            StorageOptions = storageOptions;
            ExecuteStrategy(database, fromSchema, toSchema);
            ExecuteMigration(database, fromSchema, toSchema);
        }

        protected virtual void ExecuteMigration(IMongoDatabase database, MongoSchema fromSchema, MongoSchema toSchema)
        {
            if (fromSchema > toSchema)
            {
                throw new InvalidOperationException(
                    $@"The {nameof(fromSchema)} ({fromSchema}) cannot be larger than {nameof(toSchema)} ({toSchema})");
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
                var schemaDto = new SchemaDto(schemaDoc)
                {
                    Version = migrationGroup.Key
                };
                schemas.InsertOne(schemaDto.Serialize());   
            }

            Logger.Info(() =>
                $"Instance with clientId: {StorageOptions.ClientId} is executed migration from {fromSchema} -> {toSchema} in {migrationSw.ElapsedMilliseconds}ms");
        }

        protected abstract void ExecuteStrategy(IMongoDatabase database, MongoSchema fromSchema, MongoSchema toSchema);
    }
}