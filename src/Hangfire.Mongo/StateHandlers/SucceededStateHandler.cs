using Hangfire.States;
using Hangfire.Storage;

namespace Hangfire.Mongo.StateHandlers
{
#pragma warning disable 1591
    public class SucceededStateHandler : IStateHandler
    {
        public void Apply(ApplyStateContext context, IWriteOnlyTransaction transaction)
        {
            transaction.InsertToList(State.Succeeded, context.BackgroundJob.Id);
            transaction.TrimList(State.Succeeded, 0, 99);
        }

        public void Unapply(ApplyStateContext context, IWriteOnlyTransaction transaction)
        {
            transaction.RemoveFromList(State.Succeeded, context.BackgroundJob.Id);
        }

        public string StateName => SucceededState.StateName;
    }
#pragma warning restore 1591
}