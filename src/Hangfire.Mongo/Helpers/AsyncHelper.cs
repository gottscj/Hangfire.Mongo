using System;
using System.Threading.Tasks;

namespace Hangfire.Mongo.Helpers
{
#pragma warning disable 1591
    public static class AsyncHelper
    {
        public static TResult RunSync<TResult>(Func<Task<TResult>> func)
        {
            return Task.Run(func).GetAwaiter().GetResult();
        }

        public static void RunSync(Func<Task> func)
        {
            Task.Run(func).GetAwaiter().GetResult();
        }
    }
#pragma warning restore 1591
}