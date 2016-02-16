using System;
using Hangfire.Common;
using Hangfire.States;
using Hangfire.Storage;

namespace Hangfire.Mongo.StateHandlers
{
#pragma warning disable 1591
    public class FailedStateHandler : IStateHandler
    {
        public void Apply(ApplyStateContext context, IWriteOnlyTransaction transaction)
        {
            transaction.AddToSet("failed", context.BackgroundJob.Id, JobHelper.ToTimestamp(DateTime.UtcNow));
        }

        public void Unapply(ApplyStateContext context, IWriteOnlyTransaction transaction)
        {
            transaction.RemoveFromSet("failed", context.BackgroundJob.Id);
        }

        public string StateName
        {
	        get { return FailedState.StateName; }
        }
    }
#pragma warning restore 1591
}