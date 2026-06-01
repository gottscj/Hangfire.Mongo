using System.IO;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;

namespace Hangfire.Mongo.Sample.CosmosDB
{
    public static class Program
    {
        public static void Main()
        {
            var builder = WebApplication.CreateBuilder(new WebApplicationOptions
            {
                ContentRootPath = Directory.GetCurrentDirectory()
            });
            builder.WebHost
                .UseKestrel()
                .UseContentRoot(Directory.GetCurrentDirectory())
                .ConfigureLogging(l =>
                {
//                    l.AddConsole();
//                    l.AddDebug();
                })
                .UseIISIntegration()
                .UseStartup<Startup>();
            var app = builder.Build();

            app.Run();
        }
    }
}
