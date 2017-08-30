using System;
using System.Threading;
using Hangfire.Logging;
using Hangfire.Server;

namespace Hangfire.Mongo.Signal.Mongo
{
    /// <summary>
    /// 
    /// </summary>
    public class MongoSignalManager : IBackgroundProcess, IServerComponent
    {
        private static readonly ILog Logger = LogProvider.For<ExpirationManager>();

        private readonly MongoStorage _storage;
        private readonly IPersistentSignal _signal;

        /// <summary>
        /// Constructs expiration manager with one hour checking interval
        /// </summary>
        /// <param name="storage">MongoDB storage</param>
        public MongoSignalManager(MongoStorage storage)
        {
            _storage = storage ?? throw new ArgumentNullException(nameof(storage));
            _signal = new MongoSignal(_storage.Connection.Signal);
        }

        /// <summary>
        /// Run expiration manager to remove outdated records
        /// </summary>
        /// <param name="context">Background processing context</param>
        public void Execute(BackgroundProcessContext context)
        {
            Execute(context.CancellationToken);
        }

        /// <summary>
        /// Run expiration manager to remove outdated records
        /// </summary>
        /// <param name="cancellationToken">Cancellation token</param>
        public void Execute(CancellationToken cancellationToken)
        {
            System.Diagnostics.Debug.WriteLine($@">>> Execute ({Thread.CurrentThread.ManagedThreadId})");

            using (var connection = _storage.CreateAndOpenConnection())
            {
                var mongoSignal = _signal as MongoSignal;
                mongoSignal.Listen(cancellationToken);
            }

            System.Diagnostics.Debug.WriteLine($@"<<< Execute ({Thread.CurrentThread.ManagedThreadId})");
        }

        /// <summary>
        /// Returns text representation of the object
        /// </summary>
        public override string ToString()
        {
            return "Mongo Signal Manager";
        }

    }
}