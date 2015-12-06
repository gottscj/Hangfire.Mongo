using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Web;
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
                BackgroundJob.Enqueue(() => Debug.WriteLine("Hangfire task started."));
            }

            return RedirectToAction("Index");
        }

        public ActionResult Delayed()
        {
            BackgroundJob.Schedule(() => Console.WriteLine("Delayed Hangfire task started!"),
                                   TimeSpan.FromMinutes(1));

            return RedirectToAction("Index");
        }

        public ActionResult Recurring()
        {
            RecurringJob.AddOrUpdate(() => Console.WriteLine("Recurring Hangfire task started!"),
                                     Cron.Minutely);

            return RedirectToAction("Index");
        }
    }
}