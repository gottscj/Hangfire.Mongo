using System;
using Hangfire.Mongo.Helpers;
using Xunit;

namespace Hangfire.Mongo.Tests
{
#pragma warning disable 1591
    public class AsyncHelperFacts
    {
        [Fact]
        public void UnwrapInnerException()
        {
            Assert.ThrowsAny<NotImplementedException>(() => AsyncHelper.RunSync(() =>
            {
                throw new NotImplementedException();
            }));
        }
    }
#pragma warning restore 1591
}