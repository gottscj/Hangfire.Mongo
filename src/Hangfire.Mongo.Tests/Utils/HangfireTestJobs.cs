using System;
using System.Diagnostics;
using System.Threading;

namespace Hangfire.Mongo.Tests.Utils
{
    internal class HangfireTestJobs
    {

        internal static readonly EventWaitHandle RecurringEvent = new ManualResetEvent(false);
        internal static readonly EventWaitHandle ScheduleEvent = new ManualResetEvent(false);
        internal static readonly EventWaitHandle EnqueueEvent = new ManualResetEvent(false);
        internal static readonly EventWaitHandle ContinueWithEvent = new ManualResetEvent(false);


        public static void SampleMethod(string arg)
        {
            Debug.WriteLine(arg);
        }


        public static void ExecuteRecurringJob(string argument)
        {
            Console.WriteLine(argument);
            RecurringEvent.Set();
        }


        public static void ExecuteScheduledJob(string argument)
        {
            Console.WriteLine(argument);
            ScheduleEvent.Set();
        }


        public static void ExecuteEnqueuedJob(string argument)
        {
            Console.WriteLine(argument);
            EnqueueEvent.Set();
        }


        public static void ExecuteContinueWithJob(string argument, bool continued)
        {
            Console.WriteLine(argument);
            if (continued)
            {
                ContinueWithEvent.Set();
            }
        }

    }

#pragma warning restore 1591
}