namespace Hangfire.Mongo.Migration.Steps.Version16
{
    internal class RemoveObsoleteCollections : RemoveObsoleteCollectionsStep
    {
        public override MongoSchema TargetSchema { get; } = MongoSchema.Version16;
        public override long Sequence { get; } = 99;
    }
}