using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using Hangfire.Logging;
using Hangfire.Mongo.Dto;
using Hangfire.Mongo.Migration.Steps;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Hangfire.Mongo.Migration
{

    /// <summary>
    /// Bag used for parsing info between migration steps
    /// </summary>
    internal interface IMongoMigrationContext
    {
        T GetItem<T>(string key);

        void SetItem<T>(string key, T value);
    }

    /// <summary>
    /// Class for running a full migration
    /// </summary>
    internal class MongoMigrationRunner : IMongoMigrationContext
    {
        private static readonly ILog Logger = LogProvider.For<MongoMigrationRunner>();
        
        private readonly IMongoDatabase _database;
        private readonly MongoStorageOptions _storageOptions;
        private readonly IMongoCollection<SchemaDto> _schemas;
        
        public MongoMigrationRunner(IMongoDatabase database, MongoStorageOptions storageOptions, IMongoCollection<SchemaDto> schemas)
        {
            _database = database;
            _storageOptions = storageOptions;
            _schemas = schemas;
        }


        /// <summary>
        /// Executes all migration steps between the given shemas.
        /// </summary>
        /// <param name="fromSchema">Spcifies the current shema in the database. Migration steps targeting this schema will not be executed.</param>
        /// <param name="toSchema">Specifies the schema to migrate the database to. On success this will be the schema for the database.</param>
        public void Execute(MongoSchema fromSchema, MongoSchema toSchema)
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

            

            var migrationSteps = LoadMigrationSteps()
                .Where(step => step.TargetSchema > fromSchema && step.TargetSchema <= toSchema)
                .GroupBy(step => step.TargetSchema);

            var migrationSw = Stopwatch.StartNew();
            
            foreach (var migrationGroup in migrationSteps)
            {
                Logger.Info(() =>
                        $"Executing migration for schema '{migrationGroup.Key}'");
                foreach (var migrationStep in migrationGroup)
                {
                    try
                    {
                        var sw = Stopwatch.StartNew();
                        if (!migrationStep.Execute(_database, _storageOptions, this))
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
                var schemaDto = _schemas.FindOneAndDelete(new BsonDocument()) ?? new SchemaDto();
                schemaDto.Version = migrationGroup.Key;
                _schemas.InsertOne(schemaDto);   
            }
            
            Logger.Info(() =>
                    $"Instance with clientId: {_storageOptions.ClientId} is executed migration from {fromSchema} -> {toSchema} in {migrationSw.ElapsedMilliseconds}ms");
        }


        /// <summary>
        /// Finds, instantiates and orders the migration steps available in this assembly.
        /// </summary>
        private IEnumerable<IMongoMigrationStep> LoadMigrationSteps()
        {
            var types = GetType().GetTypeInfo().Assembly.GetTypes();
            return types
                .Where(t => !t.GetTypeInfo().IsAbstract && t.GetTypeInfo().GetInterfaces().Contains(typeof(IMongoMigrationStep)))
                .Select(t => (IMongoMigrationStep)Activator.CreateInstance(t))
                .OrderBy(step => (int)step.TargetSchema).ThenBy(step => step.Sequence);
        }

        private readonly Dictionary<string, object> _environment = new Dictionary<string, object>();

        public T GetItem<T>(string key)
        {
            return (T)_environment[key];
        }

        public void SetItem<T>(string key, T value)
        {
            _environment[key] = value;
        }
    }
}
