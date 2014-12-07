using Hangfire.Mongo.Dto;
using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Linq;

namespace Hangfire.Mongo.Database
{
	public class HangfireDbContext : IDisposable
	{
		private const int RequiredSchemaVersion = 3;

		private readonly string _prefix;

		internal MongoDatabase Database { get; private set; }

		public HangfireDbContext(string	connectionString, string databaseName, string prefix = "hangfire")
		{
			_prefix = prefix;

			MongoClient client = new MongoClient(connectionString);
			MongoServer server = client.GetServer();

			Database = server.GetDatabase(databaseName);

			ConnectionId = Guid.NewGuid().ToString();
		}

		public HangfireDbContext(MongoDatabase database)
		{
			Database = database;
			ConnectionId = Guid.NewGuid().ToString();
		}

		public string ConnectionId { get; private set; }

		public virtual MongoCollection<DistributedLockDto> DistributedLock
		{
			get
			{
				return Database.GetCollection<DistributedLockDto>(_prefix + ".locks");
			}
		}
		public virtual MongoCollection<CounterDto> Counter
		{
			get
			{
				return Database.GetCollection<CounterDto>(_prefix + ".counter");
			}
		}

		public virtual MongoCollection<HashDto> Hash
		{
			get
			{
				return Database.GetCollection<HashDto>(_prefix + ".hash");
			}
		}

		public virtual MongoCollection<JobDto> Job
		{
			get
			{
				return Database.GetCollection<JobDto>(_prefix + ".job");
			}
		}

		public virtual MongoCollection<JobParameterDto> JobParameter
		{
			get
			{
				return Database.GetCollection<JobParameterDto>(_prefix + ".jobParameter");
			}
		}

		public virtual MongoCollection<JobQueueDto> JobQueue
		{
			get
			{
				return Database.GetCollection<JobQueueDto>(_prefix + ".jobQueue");
			}
		}

		public virtual MongoCollection<ListDto> List
		{
			get
			{
				return Database.GetCollection<ListDto>(_prefix + ".list");
			}
		}

		public virtual MongoCollection<SchemaDto> Schema
		{
			get
			{
				return Database.GetCollection<SchemaDto>(_prefix + ".schema");
			}
		}

		public virtual MongoCollection<ServerDto> Server
		{
			get
			{
				return Database.GetCollection<ServerDto>(_prefix + ".server");
			}
		}

		public virtual MongoCollection<SetDto> Set
		{
			get
			{
				return Database.GetCollection<SetDto>(_prefix + ".set");
			}
		}

		public virtual MongoCollection<StateDto> State
		{
			get
			{
				return Database.GetCollection<StateDto>(_prefix + ".state");
			}
		}

		public void Init()
		{
			SchemaDto schema = Schema.FindAll().FirstOrDefault();

			if (schema != null)
			{
				if (RequiredSchemaVersion > schema.Version)
				{
					Schema.RemoveAll();
					Schema.Insert(new SchemaDto { Version = RequiredSchemaVersion });
				}
				else if (RequiredSchemaVersion < schema.Version)
					throw new InvalidOperationException(String.Format("HangFire current database schema version {0} is newer than the configured MongoStorage schema version {1}. Please update to the latest HangFire.SqlServer NuGet package.",
						schema.Version, RequiredSchemaVersion));
			}
			else
				Schema.Insert(new SchemaDto {Version = RequiredSchemaVersion});
		}

		public void Dispose()
		{
		}
	}
}