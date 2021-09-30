using System;
using System.IO;

namespace Hangfire.Mongo.Sample.NETCore
{
    public class MongoRunner : IDisposable
    {
        private Mongo2Go.MongoDbRunner _runner;

        public string ConnectionString => _runner?.ConnectionString;
        public MongoRunner Start()
        {
            var homePath = Environment.OSVersion.Platform is PlatformID.Unix or PlatformID.MacOSX
                ? Environment.GetEnvironmentVariable("HOME")
                : Environment.ExpandEnvironmentVariables("%HOMEDRIVE%%HOMEPATH%");

            if (string.IsNullOrEmpty(homePath))
            {
                throw new InvalidOperationException("Could not locate home path");
            }
            var dataDir = Path.Combine(homePath, "mongodb", "data");
            // try 3 times
            for (int i = 0; i < 3; i++)
            {
                try
                {
                    _runner = Mongo2Go.MongoDbRunner.StartForDebugging(
                        singleNodeReplSet: true,
                        dataDirectory: dataDir);
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                }
            }
            

            return this;
        }

        public void Dispose()
        {
            _runner?.Dispose();
        }
    }
}