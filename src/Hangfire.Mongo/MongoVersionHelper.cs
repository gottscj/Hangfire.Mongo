using System;
using System.Linq;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Hangfire.Mongo
{
    internal static class MongoVersionHelper
    {
        public static Version GetVersion(IMongoDatabase database)
        {
            try
            {
                var command = new JsonCommand<BsonDocument>("{'buildinfo': 1}");
                var serverStatus = database.RunCommand(command);
                if (!serverStatus.Contains("version"))
                {
                    throw new InvalidOperationException("Could not get 'buildinfo' from database: got: " +
                                                        serverStatus.ToJson());
                }
            
                var versionSplit = serverStatus["version"].AsString.Split('.');
                var major = int.Parse(versionSplit[0]);
                var minor = int.Parse(versionSplit[1]);
                var build = int.Parse(new string(versionSplit[2].TakeWhile(Char.IsDigit).ToArray()));

                return new Version(major, minor, build);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Could not get 'buildinfo' from database: failed with message: " +
                                                    e.Message);
            }
        }
    }
}