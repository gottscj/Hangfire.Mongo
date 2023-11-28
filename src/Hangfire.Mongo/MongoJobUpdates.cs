using System.Collections.Generic;
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
    public List<BsonDocument> Pushes { get; } = new();

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

        if (Pushes.Any())
        {
            var pushByElement = Pushes
                .SelectMany(p => p)
                .GroupBy(elem => elem.Name)
                .Select(g => new BsonElement(g.Key, new BsonDocument("$each", new BsonArray(g.Select(e => e.Value)))));

            update["$push"] = new BsonDocument(pushByElement);
        }


        var updateModel = new UpdateOneModel<BsonDocument>(filter, update);
        return updateModel;
    }
}