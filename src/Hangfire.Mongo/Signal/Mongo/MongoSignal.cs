using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using Hangfire.Logging;
using Hangfire.Mongo.Dto;
using MongoDB.Driver;

namespace Hangfire.Mongo.Signal.Mongo
{

    /// <summary>
    /// Provide interprocess/server signalling
    /// </summary>
    internal class MongoSignal : IPersistentSignal
    {

        private static readonly ILog Logger = LogProvider.For<MongoSignal>();

        private static readonly ConcurrentDictionary<string, EventWaitHandle> EventWaitHandles = new ConcurrentDictionary<string, EventWaitHandle>();

        private IMongoCollection<SignalDto> _signalCollection;

        internal MongoSignal(IMongoCollection<SignalDto> signalCollection)
        {
            _signalCollection = signalCollection ?? throw new ArgumentNullException(nameof(signalCollection));
        }

        public void Listen(CancellationToken cancellationToken)
        {
            var options = new FindOptions<SignalDto>
            {
                // Our cursor is a tailable cursor and informs the server to await
                CursorType = CursorType.TailableAwait,
                // Make sure to time out once in a while to avoid complete blocking operation
                MaxAwaitTime = TimeSpan.FromMinutes(1),
            };

            var filterBuilder = Builders<SignalDto>.Filter;
            var filter = filterBuilder.Eq(s => s.Signaled, true);

            using (var cursor = _signalCollection.FindSync(filter, options, cancellationToken))
            {
                cancellationToken.ThrowIfCancellationRequested();
                var updateSignaled = Builders<SignalDto>.Update.Set(s => s.Signaled, false);

                cursor.ForEachAsync(document =>
                {
                    var signalDto = _signalCollection.FindOneAndUpdate(
                        s => s.Id == document.Id,
                        updateSignaled,
                        null,
                        cancellationToken);
                    if (signalDto == null)
                    {
                        Logger.Debug($@"Signal {document.Name} was trigered");
                    }
                    else
                    {
                        Logger.Debug($@"Signal {document.Name} was triggered and owned");
                    }
                    EventWaitHandles
                        .GetOrAdd(document.Name, n => new AutoResetEvent(false))
                        .Set();
                }, cancellationToken).ContinueWith(t =>
                {
                    if (t.IsFaulted)
                    {
                        var messages = string.Join(Environment.NewLine, t.Exception.InnerExceptions.Select(e => e.Message));
                        Logger.Warn($@"Signal listen is at fault: {messages}");
                    }
                }).Wait(cancellationToken);
            }
        }

        public void Set(string name)
        {
            if (name == null)
            {
                throw new ArgumentNullException(nameof(name));
            }
            if (string.IsNullOrEmpty(name))
            {
                throw new ArgumentException("Name should not be empty", nameof(name));
            }

            var update = Builders<SignalDto>.Update
                .SetOnInsert(s => s.Signaled, true)
                .SetOnInsert(s => s.Name, name)
                .CurrentDate(s => s.TimeStamp);
            _signalCollection.UpdateOne(_ => false, update, new UpdateOptions { IsUpsert = true });
        }

        public void Wait(string name)
        {
            Wait(name, TimeSpan.FromMilliseconds(-1));
        }

        public bool Wait(string name, TimeSpan timeout)
        {
            using (var cts = new CancellationTokenSource(timeout))
            {
                return Wait(name, cts.Token);
            }
        }

        public bool Wait(string name, CancellationToken cancellationToken)
        {
            if (name == null)
            {
                throw new ArgumentNullException(nameof(name));
            }
            if (string.IsNullOrEmpty(name))
            {
                throw new ArgumentException("Name should not be empty", nameof(name));
            }

            return Wait(new[] { name }, cancellationToken);
        }

        public bool Wait(string[] names, CancellationToken cancellationToken)
        {
            if (names == null)
            {
                throw new ArgumentNullException(nameof(names));
            }
            if (names.Length == 0 || names.Any(string.IsNullOrEmpty))
            {
                throw new ArgumentException("Name should not be empty", nameof(names));
            }

            var waitHandles = names
                .Select(n => EventWaitHandles.GetOrAdd(n, _ => new AutoResetEvent(false)))
                .Concat(new[] { cancellationToken.WaitHandle })
                .ToArray();

            var result = WaitHandle.WaitAny(waitHandles);
            return result < waitHandles.Length - 1;
        }

    }
}