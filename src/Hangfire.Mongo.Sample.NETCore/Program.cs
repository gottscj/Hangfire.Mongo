using System;
using Hangfire;
using Hangfire.Mongo;

namespace ConsoleApplication
{
    public class Program
    {
        public static void Main(string[] args)
        {
            JobStorage.Current = new MongoStorage("mongodb://localhost", "mymongotest");
            using (var server = new BackgroundJobServer())
            {
                for (var i = 0; i < 10; i++)
                {
                    BackgroundJob.Enqueue(() => Console.WriteLine($"Fire-and-forget ({i})"));
                }
                Console.ReadKey(true);
            }
        }
    }
}
