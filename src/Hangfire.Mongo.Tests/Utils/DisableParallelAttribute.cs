using System.Collections.Concurrent;
using System.Reflection;
using System.Threading;
using Xunit.Sdk;

namespace Hangfire.Mongo.Tests.Utils
{
    internal class DisableParallelAttribute : BeforeAfterTestAttribute
    {
        private static readonly ConcurrentDictionary<string, object> _globalLocks = new ConcurrentDictionary<string, object>();

        private object _lock;

        public DisableParallelAttribute() : this($@"{nameof(DisableParallelAttribute)}.global")
        {
        }

        public DisableParallelAttribute(string group)
        {
            _lock = _globalLocks.GetOrAdd(group, new object());
        }

        public override void Before(MethodInfo methodUnderTest)
        {
            Monitor.Enter(_lock);
        }

        public override void After(MethodInfo methodUnderTest)
        {
            Monitor.Exit(_lock);
        }

    }
}