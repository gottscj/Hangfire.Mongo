namespace Hangfire.Mongo.Migration.Steps.Version13
{
    /// <summary>
    /// Clean up obsolete collections
    /// </summary>
    internal class RemoveObsoleteCollections : RemoveObsoleteCollectionsStep
    {
        public override MongoSchema TargetSchema => MongoSchema.Version13;

        public override long Sequence => 99;
    }

}
