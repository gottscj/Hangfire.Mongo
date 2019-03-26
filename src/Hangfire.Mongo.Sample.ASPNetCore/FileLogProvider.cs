using System;
using System.Collections.Concurrent;
using System.IO;
using System.Threading.Tasks;
using Hangfire.Logging;

namespace Hangfire.Mongo.Sample.ASPNetCore
{
    public sealed class FileLogProvider : ILog, ILogProvider, IDisposable
    {
        private readonly Task _writeTask;
        private readonly BlockingCollection<string> _writeQueue;
        
        public FileLogProvider()
        {
            _writeQueue = new BlockingCollection<string>();
            _writeTask = Task.Factory.StartNew(() =>
            {
                var path = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.Desktop), "hangfire.log"); 
                
                foreach (var contents in _writeQueue.GetConsumingEnumerable())
                {
                    try
                    {
                        File.AppendAllText(path, contents);
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e);
                    }
                }
            }, TaskCreationOptions.LongRunning);
        }
        
        public bool Log(LogLevel logLevel, Func<string> messageFunc, Exception exception = null)
        {
            var message = messageFunc?.Invoke();
            if (string.IsNullOrEmpty(message))
            {
                return true;
            }
            var text = $"{DateTime.Now:hh:mm:ss:fff} - [{logLevel}] {messageFunc?.Invoke()}\r\n";
            Console.Write(text);
            _writeQueue.Add(text);
            return true;
        }

        public ILog GetLogger(string name)
        {
            return this;
        }

        public void Dispose()
        {
            _writeQueue.CompleteAdding();
            _writeQueue?.Dispose();
            _writeTask.Wait(2000);
        }
    }
}