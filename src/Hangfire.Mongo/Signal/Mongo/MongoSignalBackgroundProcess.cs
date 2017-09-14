using System.Threading;
using Hangfire.Mongo.Dto;
using Hangfire.Server;
using MongoDB.Driver;

namespace Hangfire.Mongo.Signal.Mongo
{
    /// <summary>
    /// 
    /// </summary>
    public class MongoSignalBackgroundProcess : IBackgroundProcess, IServerComponent
    {
        private readonly MongoSignal _signal;

        /// <summary>
        /// Constructs expiration manager with one hour checking interval
        /// </summary>
        /// <param name="signalCollection">MongoDB storage</param>
        public MongoSignalBackgroundProcess(IMongoCollection<SignalDto> signalCollection)
        {
            _signal = new MongoSignal(signalCollection);
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
            _signal.Listen(cancellationToken);
        }

        /// <summary>
        /// Returns text representation of the object
        /// </summary>
        public override string ToString()
        {
            return "Mongo Signal Background Process";
        }

    }
}