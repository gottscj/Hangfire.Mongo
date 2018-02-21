using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.Dto;
using Hangfire.Mongo.Migration.Steps;
using MongoDB.Driver;

namespace Hangfire.Mongo.Migration
{

    /// <summary>
    /// Bag used for parsing info between migration steps
    /// </summary>
    internal interface IMongoMigrationBag
    {
        T GetItem<T>(string key);

        void SetItem<T>(string key, T value);
    }

    /// <summary>
    /// Class for running a full migration
    /// </summary>
    internal class MongoMigrationRunner : IMongoMigrationBag
    {
        private readonly HangfireDbContext _dbContext;
        private readonly MongoStorageOptions _storageOptions;

        public MongoMigrationRunner(HangfireDbContext dbContext, MongoStorageOptions storageOptions)
        {
            _dbContext = dbContext;
            _storageOptions = storageOptions;
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

            foreach (var migrationGroup in migrationSteps)
            {
                foreach (var migrationStep in migrationGroup)
                {
                    try
                    {
                        if (!migrationStep.Execute(_dbContext.Database, _storageOptions, this))
                        {
                            throw new MongoMigrationException(migrationStep);
                        }
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
                var schemaDto = _dbContext.Schema.FindOneAndDelete(_ => true) ?? new SchemaDto();
                schemaDto.Version = migrationGroup.Key;
                _dbContext.Schema.InsertOne(schemaDto);
            }
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

        #region IMongoMigrationBag

        private Dictionary<string, object> _bag = new Dictionary<string, object>();

        public T GetItem<T>(string key)
        {
            return (T)_bag[key];
        }

        public void SetItem<T>(string key, T value)
        {
            _bag[key] = value;
        }

        #endregion
    }
}
