using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
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

        private IMongoCollection<SignalDto> _signal;

        internal MongoSignal(IMongoCollection<SignalDto> signal)
        {
            _signal = signal ?? throw new ArgumentNullException(nameof(signal));
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

            using (var cursor = _signal.FindSync(filter, options, cancellationToken))
            {
                Logger.Debug($@"*** LISTEN ({Thread.CurrentThread.ManagedThreadId})");
                cancellationToken.ThrowIfCancellationRequested();
                cursor.ForEachAsync(document =>
                {
                    var signalDto = _signal.FindOneAndUpdate(
                        s => s.Id == document.Id,
                        Builders<SignalDto>.Update.Set(s => s.Signaled, false),
                        null,
                        cancellationToken);
                    if (signalDto == null)
                    {
                        Logger.Debug($@"*** TRIGGERED ({Thread.CurrentThread.ManagedThreadId})");
                    }
                    else
                    {
                        Logger.Debug($@"*** OWNED: {signalDto.Name} ({Thread.CurrentThread.ManagedThreadId})");
                    }
                    EventWaitHandles
                        .GetOrAdd(document.Name, n => new EventWaitHandle(false, EventResetMode.AutoReset))
                        .Set();
                }, cancellationToken).ContinueWith(t =>
                {
                    // TODO: Log if canceled?
                    if (t.IsFaulted)
                    {
                        var messages = string.Join(Environment.NewLine, t.Exception.InnerExceptions.Select(e => e.Message));
                        Logger.Debug($@"*** FAULT: {messages} ({Thread.CurrentThread.ManagedThreadId})");
                    }
                }).Wait();
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
                .Set(s => s.Signaled, true)
                .Set(s => s.Name, name)
                .CurrentDate(s => s.TimeStamp);
            _signal.UpdateOne(_ => false, update, new UpdateOptions { IsUpsert = true });

            Logger.Debug($@"*** SET: {name} ({Thread.CurrentThread.ManagedThreadId})");
        }

        public void Wait(string name)
        {
            Wait(name, TimeSpan.MaxValue);
        }

        public void Wait(string name, TimeSpan timeout)
        {
            if (name == null)
            {
                throw new ArgumentNullException(nameof(name));
            }
            if (string.IsNullOrEmpty(name))
            {
                throw new ArgumentException("Name should not be empty", nameof(name));
            }

            var e = EventWaitHandles.GetOrAdd(name, n => new EventWaitHandle(false, EventResetMode.AutoReset));
            Logger.Debug($@">>> WAIT: {name} ({Thread.CurrentThread.ManagedThreadId})");
            if (timeout == TimeSpan.MaxValue)
            {
                e.WaitOne();
            }
            else
            {
                e.WaitOne(timeout);
            }
            Logger.Debug($@"<<< WAIT: {name} ({Thread.CurrentThread.ManagedThreadId})");
        }

        public void Wait(string name, CancellationToken cancellationToken)
        {
            if (name == null)
            {
                throw new ArgumentNullException(nameof(name));
            }
            if (string.IsNullOrEmpty(name))
            {
                throw new ArgumentException("Name should not be empty", nameof(name));
            }

            Logger.Debug($@">>> WAIT: {name} ({Thread.CurrentThread.ManagedThreadId})");
            Task.Run(() => Wait(name, TimeSpan.MaxValue)).Wait(cancellationToken);
            Logger.Debug($@"<<< WAIT: {name} ({Thread.CurrentThread.ManagedThreadId})");
        }

        public void Wait(string[] names, CancellationToken cancellationToken)
        {
            if (names == null)
            {
                throw new ArgumentNullException(nameof(names));
            }
            if (names.Length == 0 || names.Any(string.IsNullOrEmpty))
            {
                throw new ArgumentException("Name should not be empty", nameof(names));
            }

            Logger.Debug($@">>> WAIT: {string.Join(",", names)} ({Thread.CurrentThread.ManagedThreadId})");
            Task.WaitAny(names.Select(n => Task.Run(() => Wait(n, TimeSpan.MaxValue))).ToArray(), cancellationToken);
            Logger.Debug($@"<<< WAIT: {string.Join(",", names)} ({Thread.CurrentThread.ManagedThreadId})");
        }

    }
}