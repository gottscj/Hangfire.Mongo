using System;
using Hangfire.Common;
using Hangfire.States;
using Hangfire.Storage;

namespace Hangfire.Mongo.StateHandlers
{
#pragma warning disable 1591
    public class ProcessingStateHandler : IStateHandler
    {
        public void Apply(ApplyStateContext context, IWriteOnlyTransaction transaction)
        {
            transaction.AddToSet(State.Processing, context.BackgroundJob.Id, JobHelper.ToTimestamp(DateTime.UtcNow));
        }

        public void Unapply(ApplyStateContext context, IWriteOnlyTransaction transaction)
        {
            transaction.RemoveFromSet(State.Processing, context.BackgroundJob.Id);
        }

        public string StateName => ProcessingState.StateName;
    }
#pragma warning restore 1591
}