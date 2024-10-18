using System.IO;
using Microsoft.AspNetCore.Hosting;

namespace Hangfire.Mongo.Sample.ASPNetCore
{
    public static class Program
    {
        public static void Main()
        {
            var host = new WebHostBuilder()
                .UseKestrel()
                .UseContentRoot(Directory.GetCurrentDirectory())
                .ConfigureLogging(l =>
                {
//                    l.AddConsole();
//                    l.AddDebug();
                })
                .UseIISIntegration()
                .UseStartup<Startup>()
                .Build();

            host.Run();
        }
    }
}
