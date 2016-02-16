using Hangfire.States;
using Hangfire.Storage;

namespace Hangfire.Mongo.StateHandlers
{
#pragma warning disable 1591
    public class SucceededStateHandler : IStateHandler
    {
        public void Apply(ApplyStateContext context, IWriteOnlyTransaction transaction)
        {
            transaction.InsertToList("succeeded", context.BackgroundJob.Id);
            transaction.TrimList("succeeded", 0, 99);
        }

        public void Unapply(ApplyStateContext context, IWriteOnlyTransaction transaction)
        {
            transaction.RemoveFromList("succeeded", context.BackgroundJob.Id);
        }

        public string StateName
        {
	        get { return SucceededState.StateName; }
        }
    }
#pragma warning restore 1591
}