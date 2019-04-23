using System;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;

namespace Hangfire.Mongo.Sample.ASPNetCore.Controllers
{
    public class HomeController : Controller
    {
        public ActionResult Index()
        {
            return View();
        }

        public ActionResult FireAndForget(int id)
        {
            Parallel.ForEach(Enumerable.Range(0, id), index =>
            {
                BackgroundJob.Enqueue(() => PrintToDebug($@"Hangfire fire-and-forget task started ({index}) - {Guid.NewGuid()}"));
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
