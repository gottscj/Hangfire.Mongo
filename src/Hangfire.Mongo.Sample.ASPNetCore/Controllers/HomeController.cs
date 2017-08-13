using System;
using System.Collections.Generic;
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
            for (int i = 0; i < id; i++)
            {
                BackgroundJob.Enqueue(() => PrintToConsole("Hangfire fire-and-forget task started."));
            }

            return RedirectToAction("Index");
        }

        public ActionResult Delayed(int id)
        {
            for (int i = 0; i < id; i++)
            {
                BackgroundJob.Schedule(() => PrintToConsole("Hangfire delayed task started!"), TimeSpan.FromMinutes(1));
            }

            return RedirectToAction("Index");
        }

        public ActionResult Recurring()
        {
            RecurringJob.AddOrUpdate(() => PrintToConsole("Hangfire recurring task started!"), Cron.Minutely);

            return RedirectToAction("Index");
        }
        public static void PrintToConsole(string message)
        {
            Console.WriteLine(message);
        }
    }
}
