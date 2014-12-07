using System;

namespace Hangfire.Mongo.DistributedLock
{
	[Serializable]
	public class MongoDistributedLockException : Exception
	{
		public MongoDistributedLockException(string message)
			: base(message)
		{
		}
		public MongoDistributedLockException(string message, Exception innerException)
			: base(message, innerException)
		{
		}
	}
}