using System.Linq;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Hangfire.Mongo;

internal class MongoJobUpdates
{
    public BsonDocument Set { get; } = new();
    public BsonDocument Push { get; } = new();

    public UpdateOneModel<BsonDocument> CreateUpdateModel(string jobId)
    {
        var filter = new BsonDocument("_id", ObjectId.Parse(jobId));
        var update = new BsonDocument();
        if (Set.Any())
        {
            update["$set"] = Set;
        }

        if (Push.Any())
        {
            update["$push"] = Push;
        }
        
        
        var updateModel = new UpdateOneModel<BsonDocument>(filter, update);
        return updateModel;
    }
}