using System;
using System.Threading.Tasks;

namespace Hangfire.Mongo.Tests.Utils
{
    public static class Wait
    {
        private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);
        private static readonly TimeSpan PollInterval = TimeSpan.FromMilliseconds(20);

        /// <summary>
        /// Polls <paramref name="condition"/> until it is true or 10 seconds have passed
        /// </summary>
        public static async Task<bool> UntilAsync(Func<bool> condition)
        {
            var deadline = DateTime.UtcNow + Timeout;
            while (DateTime.UtcNow < deadline)
            {
                if (condition())
                {
                    return true;
                }

                await Task.Delay(PollInterval);
            }

            return condition();
        }

        /// <summary>
        /// Blocking variant of <see cref="UntilAsync"/> for code that must stay on the current thread,
        /// e.g. while holding a thread-affine distributed lock
        /// </summary>
        public static bool Until(Func<bool> condition)
        {
            var deadline = DateTime.UtcNow + Timeout;
            while (DateTime.UtcNow < deadline)
            {
                if (condition())
                {
                    return true;
                }

                Task.Delay(PollInterval).Wait();
            }

            return condition();
        }
    }
}
