using System.Linq;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Hangfire.Mongo;

/// <summary>
/// Updates for a specific job
/// </summary>
public class MongoJobUpdates
{
    /// <summary>
    /// Set updates
    /// </summary>
    public BsonDocument Set { get; } = new();
    
    /// <summary>
    /// Push updates
    /// </summary>
    public BsonDocument Push { get; } = new();

    /// <summary>
    /// Creates a UpdateOneModel with a filter for the given job id
    /// </summary>
    /// <param name="jobId"></param>
    /// <returns></returns>
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