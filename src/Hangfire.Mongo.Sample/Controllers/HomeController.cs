using System;
using System.Diagnostics;
using System.Web.Mvc;

namespace Hangfire.Mongo.Sample.Controllers
{
    public class HomeController : Controller
    {
        public ActionResult Index()
        {
            return View();
        }

        public ActionResult FireAndForget(int id)
        {
            for (int i = 0; i < id; i++)
            {
                BackgroundJob.Enqueue(() => PrintToDebug("Hangfire fire-and-forget task started."));
            }

            return RedirectToAction("Index");
        }

        public ActionResult Delayed(int id)
        {
            for (int i = 0; i < id; i++)
            {
                BackgroundJob.Schedule(() => PrintToDebug("Hangfire delayed task started!"), TimeSpan.FromMinutes(1));
            }

            return RedirectToAction("Index");
        }

        public ActionResult Recurring()
        {
            RecurringJob.AddOrUpdate(() => PrintToDebug("Hangfire recurring task started!"), Cron.Minutely);

            return RedirectToAction("Index");
        }
        private static void PrintToDebug(string message)
        {
            Debug.WriteLine(message);
        }
    }
}