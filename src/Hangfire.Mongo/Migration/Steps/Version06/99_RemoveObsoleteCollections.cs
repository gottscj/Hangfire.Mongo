namespace Hangfire.Mongo.Migration.Steps.Version06
{
    /// <summary>
    /// Clean up obsolete collections
    /// </summary>
    internal class RemoveObsoleteCollections : RemoveObsoleteCollectionsStep
    {
        public override MongoSchema TargetSchema => MongoSchema.Version06;

        public override long Sequence => 99;
    }

}
