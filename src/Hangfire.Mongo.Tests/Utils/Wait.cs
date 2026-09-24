using System;
using System.Threading;

namespace Hangfire.Mongo.Tests.Utils
{
    public static class Wait
    {
        /// <summary>
        /// Polls <paramref name="condition"/> until it is true or 10 seconds have passed
        /// </summary>
        public static bool Until(Func<bool> condition)
        {
            var deadline = DateTime.UtcNow.AddSeconds(10);
            while (DateTime.UtcNow < deadline)
            {
                if (condition())
                {
                    return true;
                }

                Thread.Sleep(20);
            }

            return condition();
        }
    }
}
