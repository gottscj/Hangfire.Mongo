using Hangfire.States;
using Hangfire.Storage;

namespace Hangfire.Mongo.StateHandlers
{
#pragma warning disable 1591
    public class DeletedStateHandler : IStateHandler
    {
        public void Apply(ApplyStateContext context, IWriteOnlyTransaction transaction)
        {
            transaction.InsertToList(State.Deleted, context.BackgroundJob.Id);
            transaction.TrimList(State.Deleted, 0, 99);
        }

        public void Unapply(ApplyStateContext context, IWriteOnlyTransaction transaction)
        {
            transaction.RemoveFromList(State.Deleted, context.BackgroundJob.Id);
        }

        public string StateName => DeletedState.StateName;
    }
#pragma warning restore 1591
}