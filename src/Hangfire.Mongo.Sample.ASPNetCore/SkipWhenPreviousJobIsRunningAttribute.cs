using System;
using System.Collections.Generic;
using Hangfire.Client;
using Hangfire.Common;
using Hangfire.States;
using Hangfire.Storage;

namespace Hangfire.Mongo.Sample.ASPNetCore;

// Copied from here: https://gist.github.com/odinserj/a6ad7ba6686076c9b9b2e03fcf6bf74e
// TODO: Add to Framework-Common's HangfireLocal NuGet package
public class SkipWhenPreviousJobIsRunningAttribute : JobFilterAttribute, IClientFilter, IApplyStateFilter
{
    private const string Running = "Running";
    private const string RecurringJobParam = "RecurringJobId";
    private const string KeyPrefix = "recurring-job:";
    private const string Yes = "yes";
    private const string No = "no";

    public void OnCreating(CreatingContext context)
    {
        Console.WriteLine($"OnCreating: Queue: {context.Job.Queue}, Canceled: {context.Canceled}");
        // We can't handle old storages
        if (context.Connection is not JobStorageConnection connection)
        {
            return;
        }

        // We should run this filter only for background jobs based on recurring ones
        if (!context.Parameters.ContainsKey(RecurringJobParam))
        {
            return;
        }

        var recurringJobId = context.Parameters[RecurringJobParam] as string;

        // RecurringJobId is malformed. This should not happen, but anyway.
        if (string.IsNullOrWhiteSpace(recurringJobId))
        {
            return;
        }

        var running = connection.GetValueFromHash($"{KeyPrefix}{recurringJobId}", Running);
        if (running?.Equals(Yes, StringComparison.OrdinalIgnoreCase) == true)
        {
            Console.WriteLine($"OnCreating: Setting Canceled: true");
            context.Canceled = true;
        }
    }

    public void OnCreated(CreatedContext filterContext)
    {
    }

    public void OnStateApplied(ApplyStateContext context, IWriteOnlyTransaction transaction)
    {
        Console.WriteLine($"OnStateApplied: NewState: {context.NewState.Name}");
        if (context.NewState is EnqueuedState)
        {
            var recurringJobId = SerializationHelper.Deserialize<string>(
                context.Connection.GetJobParameter(context.BackgroundJob.Id, RecurringJobParam));
            if (string.IsNullOrWhiteSpace(recurringJobId))
            {
                return;
            }

            Console.WriteLine($"OnStateApplied: Setting: {Running}:{Yes}");
            transaction.SetRangeInHash(
                $"{KeyPrefix}{recurringJobId}",
                new[] {new KeyValuePair<string, string>(Running, Yes)});
        }
        else if ((context.NewState.IsFinal &&
                  !FailedState.StateName.Equals(context.OldStateName, StringComparison.OrdinalIgnoreCase)) ||
                 (context.NewState is FailedState))
        {
            var recurringJobId =
                SerializationHelper.Deserialize<string>(
                    context.Connection.GetJobParameter(context.BackgroundJob.Id, RecurringJobParam));
            if (string.IsNullOrWhiteSpace(recurringJobId))
            {
                return;
            }
            Console.WriteLine($"OnStateApplied: Setting: {Running}:{No}");
            transaction.SetRangeInHash(
                $"{KeyPrefix}{recurringJobId}",
                new[] {new KeyValuePair<string, string>(Running, No)});
        }
    }

    public void OnStateUnapplied(ApplyStateContext context, IWriteOnlyTransaction transaction)
    {
    }
}