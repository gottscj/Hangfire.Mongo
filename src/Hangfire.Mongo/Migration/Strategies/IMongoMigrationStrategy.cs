namespace Hangfire.Mongo.Migration.Strategies
{
    internal interface IMongoMigrationStrategy
    {
        void Migrate(MongoSchema fromSchema, MongoSchema toSchema);
    }
}
