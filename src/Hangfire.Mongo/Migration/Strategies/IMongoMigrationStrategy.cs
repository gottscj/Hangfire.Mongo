namespace Hangfire.Mongo.Migration.Strategies
{
    internal interface IMongoMigrationStrategy
    {
        void Execute(MongoSchema fromSchema, MongoSchema toSchema);
    }
}
