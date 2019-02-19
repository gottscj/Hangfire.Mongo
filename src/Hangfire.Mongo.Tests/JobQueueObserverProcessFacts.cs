using System;
using System.Threading;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.Tests.Utils;
using Moq;
using Xunit;

namespace Hangfire.Mongo.Tests
{
    public class JobQueueObserverProcessFacts
    {
        private readonly HangfireDbContext _hangfireDbContext;

        private readonly CancellationToken _token;
        private readonly Mock<IJobQueueSemaphore> _jobQueueSemaphore;
        public JobQueueObserverProcessFacts()
        {
            _hangfireDbContext = ConnectionUtils.CreateDbContext();

            _token = new CancellationToken(true);
            _jobQueueSemaphore = new Mock<IJobQueueSemaphore>(MockBehavior.Strict);
        }
        
        [Fact, CleanDatabase]
        public void Execute_NoJobQueueSignals_Nothing()
        {
            var manager = new EnqueuedJobsObserver(_hangfireDbContext, _jobQueueSemaphore.Object);

            manager.Execute(_token);
        }
    }
}