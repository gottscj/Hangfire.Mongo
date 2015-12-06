using System;
using System.Threading;
using System.Threading.Tasks;

namespace Hangfire.Mongo.Helpers
{
#pragma warning disable 1591
    public static class AsyncHelper
    {
        public static TResult RunSync<TResult>(Func<Task<TResult>> func)
        {
            return Task.Run(func, CancellationToken.None).GetAwaiter().GetResult();
        }

        public static void RunSync(Func<Task> func)
        {
            Task.Run(func, CancellationToken.None).GetAwaiter().GetResult();
        }
    }
#pragma warning restore 1591
}