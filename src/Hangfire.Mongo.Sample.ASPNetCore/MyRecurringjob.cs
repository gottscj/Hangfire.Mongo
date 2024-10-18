using System;
using System.Threading;

namespace Hangfire.Mongo.Sample.ASPNetCore;

[Queue("not-default")]
[AutomaticRetry(Attempts = 0, LogEvents = true, OnAttemptsExceeded = AttemptsExceededAction.Delete)]
[SkipWhenPreviousJobIsRunning]
public class MyRecurringjob
{
    // [DisableConcurrentExecution("{0}", 3)]
    public void Recurring(string message)
    {
        Thread.Sleep(TimeSpan.FromMinutes((1)));
        Console.WriteLine(message);
    }
}