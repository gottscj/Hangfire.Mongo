// ReSharper disable InconsistentNaming

namespace Hangfire.Mongo
{
    /// <summary>
    /// Determines Hangfire.Mongo's behavior for checking if jobs are enqueued
    /// </summary>
    public enum CheckQueuedJobsStrategy
    {
        /// <summary>
        /// Will poll periodically using 'QueuePollInterval',
        /// recommended for large installments
        /// </summary>
        Poll = 0,
        /// <summary>
        /// Use change streams to watch for enqueued jobs. default setting,
        /// will still poll using 'QueuePollInterval'
        /// </summary>
        Watch = 1,
        /// <summary>
        /// Use a capped, tailable collection to notify nodes of enqueued jobs.
        /// will still poll using 'QueuePollInterval'
        /// Works with single node/ test setups
        /// </summary>
        TailNotificationsCollection = 2
    }
}