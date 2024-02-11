using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;

namespace Hangfire.Mongo.Sample.ASPNetCore.Controllers
{
    public class HomeController : Controller
    {
        public class LongRunningJobHandler
        {
            public void Execute(int index, int iterations = 100)
            {
                for (var i = 0; i < iterations; i++)
                {
                    Thread.Sleep(500);
                    Debug.WriteLine(
                        $"Hangfire long running task iteration ({(i+1)} of {iterations}) - [{Thread.CurrentThread.ManagedThreadId}]");
                }
                
            }
        }
        public class JobHandler
        {
            public void Execute(int index)
            {
                Thread.Sleep(200);
                Debug.WriteLine(
                    $@"Hangfire fire-and-forget task started ({index}) - [{Thread.CurrentThread.ManagedThreadId}]");
            }
        }

        public class RandomExceptionJobHandler
        {
            public void Execute(int index)
            {
                Thread.Sleep(200);
                var rand = new Random();
                var next = rand.Next(1, 100);
                var shouldThrow = next > 50;
                if (shouldThrow)
                {
                    throw new InvalidOperationException($"Throwing exception as {next} > 50 = true");
                }
                Debug.WriteLine(
                    $@"Hangfire random exception task started ({index}) - [{Thread.CurrentThread.ManagedThreadId}]");
            }
        }

        public ActionResult Index()
        {
            return View();
        }

        public ActionResult LongRunning(int id, int iterations)
        {
            BackgroundJob.Enqueue<LongRunningJobHandler>(j => j.Execute(id, iterations)); 
            return RedirectToAction("Index");
        }
        
        public ActionResult RandomException(int id)
        {
            BackgroundJob.Enqueue<RandomExceptionJobHandler>(j => j.Execute(id)); 
            return RedirectToAction("Index");
        }

        public ActionResult FireAndForget(int id)
        {
            Parallel.ForEach(Enumerable.Range(0, id),
                index => { BackgroundJob.Enqueue<JobHandler>(j => j.Execute(index)); });

            return RedirectToAction("Index");
        }

        public ActionResult Delayed(int id)
        {
            Parallel.ForEach(Enumerable.Range(0, id),
                index =>
                {
                    BackgroundJob.Schedule(
                        () => PrintToDebug($@"Hangfire delayed task started ({index}) - {Guid.NewGuid()}"),
                        TimeSpan.FromMinutes(1));
                });

            return RedirectToAction("Index");
        }

        public ActionResult Recurring()
        {
            RecurringJob.AddOrUpdate<MyRecurringjob>("my-recurring-job",
                j => j.Recurring($@"Hangfire recurring task started - {Guid.NewGuid()}"), Cron.Minutely);

            return RedirectToAction("Index");
        }

        public static void PrintToDebug(string message)
        {
            Debug.WriteLine(message);
        }
        
    }
}