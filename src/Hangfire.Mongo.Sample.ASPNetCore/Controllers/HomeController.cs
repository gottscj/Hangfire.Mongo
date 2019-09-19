using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Hangfire.Storage.Monitoring;
using Microsoft.AspNetCore.Mvc;

namespace Hangfire.Mongo.Sample.ASPNetCore.Controllers
{
    public class HomeController : Controller
    {
        public class JobHandler
        {
            public void Execute(int index)
            {
                Thread.Sleep(200);
                Debug.WriteLine($@"Hangfire fire-and-forget task started ({index}) - [{Thread.CurrentThread.ManagedThreadId}]");
            }
        }
        public ActionResult Index()
        {
            return View();
        }

        public ActionResult FireAndForget(int id)
        {
            Parallel.ForEach(Enumerable.Range(0, id), index =>
            {
                BackgroundJob.Enqueue<JobHandler>(j => j.Execute(index));
            });

            return RedirectToAction("Index");
        }

        public ActionResult Delayed(int id)
        {
            Parallel.ForEach(Enumerable.Range(0, id), index =>
            {
                BackgroundJob.Schedule(() => PrintToDebug($@"Hangfire delayed task started ({index}) - {Guid.NewGuid()}"), TimeSpan.FromMinutes(1));
            });

            return RedirectToAction("Index");
        }

        public ActionResult Recurring()
        {
            
            RecurringJob.AddOrUpdate(() => PrintToDebug($@"Hangfire recurring task started - {Guid.NewGuid()}"), Cron.Minutely);

            return RedirectToAction("Index");
        }

        public static void PrintToDebug(string message)
        {
            Debug.WriteLine(message);
        }
    }
}
