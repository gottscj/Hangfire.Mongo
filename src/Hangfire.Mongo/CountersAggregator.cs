using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Hangfire.Logging;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.Dto;
using Hangfire.Server;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Hangfire.Mongo
{
    /// <summary>
    /// Represents Counter collection aggregator for Mongo database
    /// </summary>
    public class CountersAggregator : IBackgroundProcess, IServerComponent
    {
        private static readonly ILog Logger = LogProvider.GetCurrentClassLogger();

        private const int NumberOfRecordsInSinglePass = 1000;
        private static readonly TimeSpan DelayBetweenPasses = TimeSpan.FromMilliseconds(500);

        private readonly MongoStorage _storage;
        private readonly TimeSpan _interval;

        /// <summary>
        /// Constructs Counter collection aggregator
        /// </summary>
        /// <param name="storage">MongoDB storage</param>
        /// <param name="interval">Checking interval</param>
        public CountersAggregator(MongoStorage storage, TimeSpan interval)
        {
            if (storage == null)
                throw new ArgumentNullException("storage");

            _storage = storage;
            _interval = interval;
        }

        /// <summary>
        /// Runs aggregator
        /// </summary>
        /// <param name="context">Background processing context</param>
        public void Execute(BackgroundProcessContext context)
        {
            Execute(context.CancellationToken);
        }

        /// <summary>
        /// Runs aggregator
        /// </summary>
        /// <param name="cancellationToken">Cancellation token</param>
        public void Execute(CancellationToken cancellationToken)
        {
            Logger.DebugFormat("Aggregating records in 'Counter' table...");

            long removedCount;

            do
            {
                using (var storageConnection = (MongoConnection)_storage.GetConnection())
                {
                    HangfireDbContext database = storageConnection.Database;

                    List<CounterDto> recordsToAggregate = database
						.Counter.Find(new BsonDocument())
						.Limit(NumberOfRecordsInSinglePass)
						.ToList();

                    var recordsToMerge = recordsToAggregate
                        .GroupBy(_ => _.Key).Select(_ => new
                        {
                            Key = _.Key,
                            Value = _.Sum(x => x.Value),
                            ExpireAt = _.Max(x => x.ExpireAt)
                        });

                    foreach (var item in recordsToMerge)
                    {
                        AggregatedCounterDto aggregatedItem = database
							.AggregatedCounter
							.Find(Builders<AggregatedCounterDto>.Filter.Eq(_ => _.Key, item.Key))
							.FirstOrDefault();
                        if (aggregatedItem != null)
                        {
                            database.AggregatedCounter.UpdateOne(Builders<AggregatedCounterDto>.Filter.Eq(_ => _.Key, item.Key),
                                Builders<AggregatedCounterDto>.Update.Combine(
                                Builders<AggregatedCounterDto>.Update.Inc(_ => _.Value, item.Value),
                                Builders<AggregatedCounterDto>.Update.Set(_ => _.ExpireAt, item.ExpireAt > aggregatedItem.ExpireAt ? item.ExpireAt : aggregatedItem.ExpireAt)));
                        }
                        else
                        {
                           database.AggregatedCounter.InsertOne(new AggregatedCounterDto
                            {
                                Id = ObjectId.GenerateNewId(),
                                Key = item.Key,
                                Value = item.Value,
                                ExpireAt = item.ExpireAt
                            });
                        }
                    }

                    removedCount = database
						.Counter
						.DeleteMany(Builders<CounterDto>.Filter.In(_ => _.Id, recordsToAggregate.Select(_ => _.Id)))
						.DeletedCount;
                }

                if (removedCount >= NumberOfRecordsInSinglePass)
                {
                    cancellationToken.WaitHandle.WaitOne(DelayBetweenPasses);
                    cancellationToken.ThrowIfCancellationRequested();
                }
            } while (removedCount >= NumberOfRecordsInSinglePass);

            cancellationToken.WaitHandle.WaitOne(_interval);
        }

        /// <summary>
        /// Returns text representation of the object
        /// </summary>
        public override string ToString()
        {
            return "MongoDB Counter Colleciton Aggregator";
        }
    }
}