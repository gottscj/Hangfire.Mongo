using System;
using System.Collections.Generic;
using System.Linq;
using Hangfire.Logging;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.Dto;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Hangfire.Mongo.CosmosDB;

#pragma warning disable 1591

public class CosmosDbWriteOnlyTransaction : MongoWriteOnlyTransaction
{
    public CosmosDbWriteOnlyTransaction(HangfireDbContext dbContext, MongoStorageOptions storageOptions) : base(
        dbContext, storageOptions)
    {
    }

    /// <summary>
    /// check if we are inserting a Job, add the "Key" field as this is required in cosmos as there is
    /// unique index for this field
    /// </summary>
    /// <param name="jobGraph"></param>
    /// <param name="writeModels"></param>
    /// <param name="bulkWriteOptions"></param>
    protected override void ExecuteCommit(IMongoCollection<BsonDocument> jobGraph, List<WriteModel<BsonDocument>> writeModels, BulkWriteOptions bulkWriteOptions)
    {
        foreach (var insertOneModel in writeModels.OfType<InsertOneModel<BsonDocument>>())
        {
            var typeArray = insertOneModel.Document["_t"].AsBsonArray;
            if (typeArray.Contains("JobDto"))
            {
                insertOneModel.Document[nameof(KeyJobDto.Key)] = insertOneModel.Document["_id"].ToString();
            }
        }
        
        var trys = 3;
        do
        {
            try
            {
                base.ExecuteCommit(jobGraph, writeModels, bulkWriteOptions);
                break;
            }
            catch (Exception e)
            {
                trys -= 1;
                Logger.WarnException("Error writing to DB", e);
                if (trys == 0)
                {
                    Logger.ErrorException("Throwing after 3 re-trys", e);
                    throw;
                }
            }
        } while (true);
    }
}

#pragma warning restore 1591