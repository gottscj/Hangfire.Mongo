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
                BackgroundJob.Enqueue(() => Debug.WriteLine("Hangfire fire-and-forget task started."));
            }

            return RedirectToAction("Index");
        }

        public ActionResult Delayed(int id)
        {
            for (int i = 0; i < id; i++)
            {
                BackgroundJob.Schedule(() => Console.WriteLine("Hangfire delayed task started!"),
                                       TimeSpan.FromMinutes(1));
            }

            return RedirectToAction("Index");
        }

        public ActionResult Recurring()
        {
            RecurringJob.AddOrUpdate(() => Console.WriteLine("Hangfire recurring task started!"),
                                     Cron.Minutely);

            return RedirectToAction("Index");
        }
    }
}