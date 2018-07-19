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
        private readonly Mock<JobQueueSemaphore> _jobQueueSemaphore;
        public JobQueueObserverProcessFacts()
        {
            _hangfireDbContext = ConnectionUtils.CreateDbContext();

            _token = new CancellationToken(true);
            _jobQueueSemaphore = new Mock<JobQueueSemaphore>(MockBehavior.Strict);
        }
        
        [Fact]
        public void Ctor_ThrowsAnException_WhenStorageIsNull()
        {
            Assert.Throws<ArgumentNullException>(() => new EnqueuedJobsObserver(null, _jobQueueSemaphore.Object));
        }

        [Fact, CleanDatabase]
        public void Execute_NoJobQueueSignals_Nothing()
        {
            var manager = new EnqueuedJobsObserver(_hangfireDbContext, _jobQueueSemaphore.Object);

            manager.Execute(_token);
        }
    }
}