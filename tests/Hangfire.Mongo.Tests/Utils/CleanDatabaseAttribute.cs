using System;
using System.Data.SqlClient;
using System.Reflection;
using System.Threading;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.Dto;
using MongoDB.Driver;
using Xunit;

namespace Hangfire.Mongo.Tests.Utils
{
	public class CleanDatabaseAttribute : BeforeAfterTestAttribute
	{
		private static readonly object GlobalLock = new object();

		private static bool _sqlObjectInstalled;

		public CleanDatabaseAttribute()
		{
		}

		public override void Before(MethodInfo methodUnderTest)
		{
			Monitor.Enter(GlobalLock);

			if (_sqlObjectInstalled == false)
			{
				RecreateDatabaseAndInstallObjects();
				_sqlObjectInstalled = true;
			}
		}

		public override void After(MethodInfo methodUnderTest)
		{
			Monitor.Exit(GlobalLock);
		}

		private static void RecreateDatabaseAndInstallObjects()
		{
			using (HangfireDbContext context = new HangfireDbContext(ConnectionUtils.GetConnectionString(), ConnectionUtils.GetDatabaseName()))
			{
				context.Init();

				MongoCollection[] collections =
				{
					context.DistributedLock,
					context.Counter,
					context.Hash,
					context.Job,
					context.JobParameter,
					context.JobQueue,
					context.List,
					context.Server,
					context.Set,
					context.State
				};

				foreach (MongoCollection collection in collections)
				{
					WriteConcernResult result = collection.RemoveAll();
					if (result.Ok == false)
						throw new InvalidOperationException("Unable to cleanup database.");
				}
			}
		}
	}
}
