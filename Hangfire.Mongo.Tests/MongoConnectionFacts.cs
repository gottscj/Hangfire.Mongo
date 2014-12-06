using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Hangfire.Common;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.Dto;
using Hangfire.Mongo.MongoUtils;
using Hangfire.Mongo.PersistentJobQueue;
using Hangfire.Mongo.Tests.Utils;
using Hangfire.Server;
using Hangfire.Storage;
using MongoDB.Bson;
using MongoDB.Driver.Builders;
using Moq;
using Xunit;

namespace Hangfire.Mongo.Tests
{
	public class MongoConnectionFacts
	{
		private readonly Mock<IPersistentJobQueue> _queue;
		private readonly Mock<IPersistentJobQueueProvider> _provider;
		private readonly PersistentJobQueueProviderCollection _providers;

		public MongoConnectionFacts()
		{
			_queue = new Mock<IPersistentJobQueue>();

			_provider = new Mock<IPersistentJobQueueProvider>();
			_provider.Setup(x => x.GetJobQueue(It.IsNotNull<HangfireDbContext>())).Returns(_queue.Object);

			_providers = new PersistentJobQueueProviderCollection(_provider.Object);
		}

		[Fact]
		public void Ctor_ThrowsAnException_WhenConnectionIsNull()
		{
			var exception = Assert.Throws<ArgumentNullException>(
				() => new MongoConnection(null, _providers));

			Assert.Equal("database", exception.ParamName);
		}

		[Fact, CleanDatabase]
		public void Ctor_ThrowsAnException_WhenProvidersCollectionIsNull()
		{
			var exception = Assert.Throws<ArgumentNullException>(
				() => new MongoConnection(ConnectionUtils.CreateConnection(), null));

			Assert.Equal("queueProviders", exception.ParamName);
		}


		[Fact, CleanDatabase]
		public void FetchNextJob_DelegatesItsExecution_ToTheQueue()
		{
			UseConnection((database, connection) =>
			{
				var token = new CancellationToken();
				var queues = new[] { "default" };

				connection.FetchNextJob(queues, token);

				_queue.Verify(x => x.Dequeue(queues, token));
			});
		}

		[Fact, CleanDatabase]
		public void FetchNextJob_Throws_IfMultipleProvidersResolved()
		{
			UseConnection((database, connection) =>
			{
				var token = new CancellationToken();
				var anotherProvider = new Mock<IPersistentJobQueueProvider>();
				_providers.Add(anotherProvider.Object, new[] { "critical" });

				Assert.Throws<InvalidOperationException>(
					() => connection.FetchNextJob(new[] { "critical", "default" }, token));
			});
		}

		[Fact, CleanDatabase]
		public void CreateWriteTransaction_ReturnsNonNullInstance()
		{
			UseConnection((database, connection) =>
			{
				var transaction = connection.CreateWriteTransaction();
				Assert.NotNull(transaction);
			});
		}

		[Fact, CleanDatabase]
		public void AcquireLock_ReturnsNonNullInstance()
		{
			UseConnection((database, connection) =>
			{
				var @lock = connection.AcquireDistributedLock("1", TimeSpan.FromSeconds(1));
				Assert.NotNull(@lock);
			});
		}

		[Fact, CleanDatabase]
		public void CreateExpiredJob_ThrowsAnException_WhenJobIsNull()
		{
			UseConnection((database, connection) =>
			{
				var exception = Assert.Throws<ArgumentNullException>(
					() => connection.CreateExpiredJob(
						null,
						new Dictionary<string, string>(),
						database.GetServerTimeUtc(),
						TimeSpan.Zero));

				Assert.Equal("job", exception.ParamName);
			});
		}

		[Fact, CleanDatabase]
		public void CreateExpiredJob_ThrowsAnException_WhenParametersCollectionIsNull()
		{
			UseConnection((database, connection) =>
			{
				var exception = Assert.Throws<ArgumentNullException>(
					() => connection.CreateExpiredJob(
						Job.FromExpression(() => SampleMethod("hello")),
						null,
						database.GetServerTimeUtc(),
						TimeSpan.Zero));

				Assert.Equal("parameters", exception.ParamName);
			});
		}

		[Fact, CleanDatabase]
		public void CreateExpiredJob_CreatesAJobInTheStorage_AndSetsItsParameters()
		{
			UseConnection((database, connection) =>
			{
				var createdAt = new DateTime(2012, 12, 12, 0, 0, 0, 0, DateTimeKind.Utc);
				var jobId = connection.CreateExpiredJob(Job.FromExpression(() => SampleMethod("Hello")),
					new Dictionary<string, string> { { "Key1", "Value1" }, { "Key2", "Value2" } },
					createdAt,
					TimeSpan.FromDays(1));

				Assert.NotNull(jobId);
				Assert.NotEmpty(jobId);

				var databaseJob = database.Job.FindAll().Single();
				Assert.Equal(jobId, databaseJob.Id.ToString());
				Assert.Equal(createdAt, databaseJob.CreatedAt);
				Assert.Equal(ObjectId.Empty, databaseJob.StateId);
				Assert.Equal(null, databaseJob.StateName);

				var invocationData = JobHelper.FromJson<InvocationData>((string)databaseJob.InvocationData);
				invocationData.Arguments = databaseJob.Arguments;

				var job = invocationData.Deserialize();
				Assert.Equal(typeof(MongoConnectionFacts), job.Type);
				Assert.Equal("SampleMethod", job.Method.Name);
				Assert.Equal("\"Hello\"", job.Arguments[0]);

				Assert.True(createdAt.AddDays(1).AddMinutes(-1) < databaseJob.ExpireAt);
				Assert.True(databaseJob.ExpireAt < createdAt.AddDays(1).AddMinutes(1));

				var parameters = database.JobParameter.Find(Query<JobParameterDto>.EQ(_ => _.JobId, int.Parse(jobId)))
					.ToDictionary(x => x.Name, x => x.Value);

				Assert.Equal("Value1", parameters["Key1"]);
				Assert.Equal("Value2", parameters["Key2"]);
			});
		}

		[Fact, CleanDatabase]
		public void GetJobData_ThrowsAnException_WhenJobIdIsNull()
		{
			UseConnection((database, connection) => Assert.Throws<ArgumentNullException>(
					() => connection.GetJobData(null)));
		}

		[Fact, CleanDatabase]
		public void GetJobData_ReturnsNull_WhenThereIsNoSuchJob()
		{
			UseConnection((database, connection) =>
			{
				var result = connection.GetJobData("547527b4c6b6cc26a02d021d");
				Assert.Null(result);
			});
		}

		[Fact, CleanDatabase]
		public void GetJobData_ReturnsResult_WhenJobExists()
		{
			UseConnection((database, connection) =>
			{
				var job = Job.FromExpression(() => SampleMethod("wrong"));

				var jobDto = new JobDto
				{
					Id = 1,
					InvocationData = JobHelper.ToJson(InvocationData.Serialize(job)),
					Arguments = "['Arguments']",
					StateName = "Succeeded",
					CreatedAt = database.GetServerTimeUtc()
				};
				database.Job.Insert(jobDto);

				var result = connection.GetJobData(jobDto.Id.ToString());

				Assert.NotNull(result);
				Assert.NotNull(result.Job);
				Assert.Equal("Succeeded", result.State);
				Assert.Equal("Arguments", result.Job.Arguments[0]);
				Assert.Null(result.LoadException);
				Assert.True(database.GetServerTimeUtc().AddMinutes(-1) < result.CreatedAt);
				Assert.True(result.CreatedAt < DateTime.UtcNow.AddMinutes(1));
			});
		}

		[Fact, CleanDatabase]
		public void GetStateData_ThrowsAnException_WhenJobIdIsNull()
		{
			UseConnection(
				(database, connection) => Assert.Throws<ArgumentNullException>(
					() => connection.GetStateData(null)));
		}

		[Fact, CleanDatabase]
		public void GetStateData_ReturnsNull_IfThereIsNoSuchState()
		{
			UseConnection((database, connection) =>
			{
				var result = connection.GetStateData("547527b4c6b6cc26a02d021d");
				Assert.Null(result);
			});
		}

		[Fact, CleanDatabase]
		public void GetStateData_ReturnsCorrectData()
		{
			UseConnection((database, connection) =>
			{
				var data = new Dictionary<string, string>
						{
							{ "Key", "Value" }
						};

				var jobDto = new JobDto
				{
					Id = 1,
					InvocationData = "",
					Arguments = "",
					StateName = "",
					CreatedAt = database.GetServerTimeUtc()
				};

				database.Job.Insert(jobDto);
				var jobId = jobDto.Id;

				database.State.Insert(new StateDto
				{
					Id = ObjectId.GenerateNewId(),
					JobId = jobId,
					Name = "old-state",
					CreatedAt = database.GetServerTimeUtc()
				});

				var stateDto = new StateDto
				{
					Id = ObjectId.GenerateNewId(),
					JobId = jobId,
					Name = "Name",
					Reason = "Reason",
					Data = JobHelper.ToJson(data),
					CreatedAt = database.GetServerTimeUtc()
				};
				database.State.Insert(stateDto);

				jobDto.StateId = stateDto.Id;
				database.Job.Save(jobDto);

				var result = connection.GetStateData(jobId.ToString());
				Assert.NotNull(result);

				Assert.Equal("Name", result.Name);
				Assert.Equal("Reason", result.Reason);
				Assert.Equal("Value", result.Data["Key"]);
			});
		}

		[Fact, CleanDatabase]
		public void GetJobData_ReturnsJobLoadException_IfThereWasADeserializationException()
		{
			UseConnection((database, connection) =>
			{
				var jobDto = new JobDto
				{
					Id = 1,
					InvocationData = JobHelper.ToJson(new InvocationData(null, null, null, null)),
					Arguments = "['Arguments']",
					StateName = "Succeeded",
					CreatedAt = database.GetServerTimeUtc()
				};
				database.Job.Insert(jobDto);
				var jobId = jobDto.Id;

				var result = connection.GetJobData(jobId.ToString());

				Assert.NotNull(result.LoadException);
			});
		}

		[Fact, CleanDatabase]
		public void SetParameter_ThrowsAnException_WhenJobIdIsNull()
		{
			UseConnection((database, connection) =>
			{
				var exception = Assert.Throws<ArgumentNullException>(
					() => connection.SetJobParameter(null, "name", "value"));

				Assert.Equal("id", exception.ParamName);
			});
		}

		[Fact, CleanDatabase]
		public void SetParameter_ThrowsAnException_WhenNameIsNull()
		{
			UseConnection((database, connection) =>
			{
				var exception = Assert.Throws<ArgumentNullException>(
					() => connection.SetJobParameter("547527b4c6b6cc26a02d021d", null, "value"));

				Assert.Equal("name", exception.ParamName);
			});
		}

		[Fact, CleanDatabase]
		public void SetParameters_CreatesNewParameter_WhenParameterWithTheGivenNameDoesNotExists()
		{
			UseConnection((database, connection) =>
			{
				var jobDto = new JobDto
				{
					Id = 1,
					InvocationData = "",
					Arguments = "",
					CreatedAt = database.GetServerTimeUtc()
				};
				database.Job.Insert(jobDto);
				string jobId = jobDto.Id.ToString();

				connection.SetJobParameter(jobId, "Name", "Value");

				var parameter = database.JobParameter.FindOne(Query.And(Query<JobParameterDto>.EQ(_ => _.JobId, int.Parse(jobId)),
					Query<JobParameterDto>.EQ(_ => _.Name, "Name")));

				Assert.Equal("Value", parameter.Value);
			});
		}

		[Fact, CleanDatabase]
		public void SetParameter_UpdatesValue_WhenParameterWithTheGivenName_AlreadyExists()
		{
			UseConnection((database, connection) =>
			{
				var jobDto = new JobDto
				{
					Id = 1,
					InvocationData = "",
					Arguments = "",
					CreatedAt = database.GetServerTimeUtc()
				};
				database.Job.Insert(jobDto);
				string jobId = jobDto.Id.ToString();

				connection.SetJobParameter(jobId, "Name", "Value");
				connection.SetJobParameter(jobId, "Name", "AnotherValue");

				var parameter = database.JobParameter.FindOne(Query.And(Query<JobParameterDto>.EQ(_ => _.JobId, int.Parse(jobId)),
					Query<JobParameterDto>.EQ(_ => _.Name, "Name")));

				Assert.Equal("AnotherValue", parameter.Value);
			});
		}

		[Fact, CleanDatabase]
		public void SetParameter_CanAcceptNulls_AsValues()
		{
			UseConnection((database, connection) =>
			{
				var jobDto = new JobDto
				{
					Id = 1,
					InvocationData = "",
					Arguments = "",
					CreatedAt = database.GetServerTimeUtc()
				};
				database.Job.Insert(jobDto);
				string jobId = jobDto.Id.ToString();

				connection.SetJobParameter(jobId, "Name", null);

				var parameter = database.JobParameter.FindOne(Query.And(Query<JobParameterDto>.EQ(_ => _.JobId, int.Parse(jobId)),
					Query<JobParameterDto>.EQ(_ => _.Name, "Name")));

				Assert.Equal(null, parameter.Value);
			});
		}

		[Fact, CleanDatabase]
		public void GetParameter_ThrowsAnException_WhenJobIdIsNull()
		{
			UseConnection((database, connection) =>
			{
				var exception = Assert.Throws<ArgumentNullException>(
					() => connection.GetJobParameter(null, "hello"));

				Assert.Equal("id", exception.ParamName);
			});
		}

		[Fact, CleanDatabase]
		public void GetParameter_ThrowsAnException_WhenNameIsNull()
		{
			UseConnection((database, connection) =>
			{
				var exception = Assert.Throws<ArgumentNullException>(
					() => connection.GetJobParameter("547527b4c6b6cc26a02d021d", null));

				Assert.Equal("name", exception.ParamName);
			});
		}

		[Fact, CleanDatabase]
		public void GetParameter_ReturnsNull_WhenParameterDoesNotExists()
		{
			UseConnection((database, connection) =>
			{
				var value = connection.GetJobParameter("1", "hello");
				Assert.Null(value);
			});
		}

		[Fact, CleanDatabase]
		public void GetParameter_ReturnsParameterValue_WhenJobExists()
		{
			UseConnection((database, connection) =>
			{
				var jobDto = new JobDto
				{
					Id = 1,
					InvocationData = "",
					Arguments = "",
					CreatedAt = database.GetServerTimeUtc()
				};
				database.Job.Insert(jobDto);
				string jobId = jobDto.Id.ToString();

				database.JobParameter.Insert(new JobParameterDto
				{
					Id = ObjectId.GenerateNewId(),
					JobId = int.Parse(jobId),
					Name = "name",
					Value = "value"
				});

				var value = connection.GetJobParameter(jobId, "name");

				Assert.Equal("value", value);
			});
		}

		[Fact, CleanDatabase]
		public void GetFirstByLowestScoreFromSet_ThrowsAnException_WhenKeyIsNull()
		{
			UseConnection((database, connection) =>
			{
				var exception = Assert.Throws<ArgumentNullException>(
					() => connection.GetFirstByLowestScoreFromSet(null, 0, 1));

				Assert.Equal("key", exception.ParamName);
			});
		}

		[Fact, CleanDatabase]
		public void GetFirstByLowestScoreFromSet_ThrowsAnException_ToScoreIsLowerThanFromScore()
		{
			UseConnection((database, connection) => Assert.Throws<ArgumentException>(
				() => connection.GetFirstByLowestScoreFromSet("key", 0, -1)));
		}

		[Fact, CleanDatabase]
		public void GetFirstByLowestScoreFromSet_ReturnsNull_WhenTheKeyDoesNotExist()
		{
			UseConnection((database, connection) =>
			{
				var result = connection.GetFirstByLowestScoreFromSet(
					"key", 0, 1);

				Assert.Null(result);
			});
		}

		[Fact, CleanDatabase]
		public void GetFirstByLowestScoreFromSet_ReturnsTheValueWithTheLowestScore()
		{
			UseConnection((database, connection) =>
			{
				database.Set.Insert(new SetDto
				{
					Id = ObjectId.GenerateNewId(),
					Key = "key",
					Score = 1.0,
					Value = "1.0"
				});
				database.Set.Insert(new SetDto
				{
					Id = ObjectId.GenerateNewId(),
					Key = "key",
					Score = -1.0,
					Value = "-1.0"
				});
				database.Set.Insert(new SetDto
				{
					Id = ObjectId.GenerateNewId(),
					Key = "key",
					Score = -5.0,
					Value = "-5.0"
				});
				database.Set.Insert(new SetDto
				{
					Id = ObjectId.GenerateNewId(),
					Key = "another-key",
					Score = -2.0,
					Value = "-2.0"
				});

				var result = connection.GetFirstByLowestScoreFromSet("key", -1.0, 3.0);

				Assert.Equal("-1.0", result);
			});
		}

		[Fact, CleanDatabase]
		public void AnnounceServer_ThrowsAnException_WhenServerIdIsNull()
		{
			UseConnection((database, connection) =>
			{
				var exception = Assert.Throws<ArgumentNullException>(
					() => connection.AnnounceServer(null, new ServerContext()));

				Assert.Equal("serverId", exception.ParamName);
			});
		}

		[Fact, CleanDatabase]
		public void AnnounceServer_ThrowsAnException_WhenContextIsNull()
		{
			UseConnection((database, connection) =>
			{
				var exception = Assert.Throws<ArgumentNullException>(
					() => connection.AnnounceServer("server", null));

				Assert.Equal("context", exception.ParamName);
			});
		}

		[Fact, CleanDatabase]
		public void AnnounceServer_CreatesOrUpdatesARecord()
		{
			UseConnection((database, connection) =>
			{
				var context1 = new ServerContext
				{
					Queues = new[] { "critical", "default" },
					WorkerCount = 4
				};
				connection.AnnounceServer("server", context1);

				var server = database.Server.FindAll().Single();
				Assert.Equal("server", server.Id);
				Assert.True(((string)server.Data).StartsWith(
					"{\"WorkerCount\":4,\"Queues\":[\"critical\",\"default\"],\"StartedAt\":"),
					server.Data);
				Assert.NotNull(server.LastHeartbeat);

				var context2 = new ServerContext
				{
					Queues = new[] { "default" },
					WorkerCount = 1000
				};
				connection.AnnounceServer("server", context2);
				var sameServer = database.Server.FindAll().Single();
				Assert.Equal("server", sameServer.Id);
				Assert.Contains("1000", sameServer.Data);
			});
		}

		[Fact, CleanDatabase]
		public void RemoveServer_ThrowsAnException_WhenServerIdIsNull()
		{
			UseConnection((database, connection) => Assert.Throws<ArgumentNullException>(
				() => connection.RemoveServer(null)));
		}

		[Fact, CleanDatabase]
		public void RemoveServer_RemovesAServerRecord()
		{
			UseConnection((database, connection) =>
			{
				database.Server.Insert(new ServerDto
				{
					Id = "Server1",
					Data = "",
					LastHeartbeat = database.GetServerTimeUtc()
				});
				database.Server.Insert(new ServerDto
				{
					Id = "Server2",
					Data = "",
					LastHeartbeat = database.GetServerTimeUtc()
				});

				connection.RemoveServer("Server1");

				var server = database.Server.FindAll().Single();
				Assert.NotEqual("Server1", server.Id, StringComparer.OrdinalIgnoreCase);
			});
		}

		[Fact, CleanDatabase]
		public void Heartbeat_ThrowsAnException_WhenServerIdIsNull()
		{
			UseConnection((database, connection) => Assert.Throws<ArgumentNullException>(
				() => connection.Heartbeat(null)));
		}

		[Fact, CleanDatabase]
		public void Heartbeat_UpdatesLastHeartbeat_OfTheServerWithGivenId()
		{
			UseConnection((database, connection) =>
			{
				database.Server.Insert(new ServerDto
				{
					Id = "server1",
					Data = "",
					LastHeartbeat = new DateTime(2012, 12, 12, 12, 12, 12, DateTimeKind.Utc)
				});
				database.Server.Insert(new ServerDto
				{
					Id = "server2",
					Data = "",
					LastHeartbeat = new DateTime(2012, 12, 12, 12, 12, 12, DateTimeKind.Utc)
				});

				connection.Heartbeat("server1");

				var servers = database.Server.FindAll()
					.ToDictionary(x => x.Id, x => x.LastHeartbeat);

				Assert.NotEqual(2012, servers["server1"].Year);
				Assert.Equal(2012, servers["server2"].Year);
			});
		}

		[Fact, CleanDatabase]
		public void RemoveTimedOutServers_ThrowsAnException_WhenTimeOutIsNegative()
		{
			UseConnection((database, connection) => Assert.Throws<ArgumentException>(
				() => connection.RemoveTimedOutServers(TimeSpan.FromMinutes(-5))));
		}

		[Fact, CleanDatabase]
		public void RemoveTimedOutServers_DoItsWorkPerfectly()
		{
			UseConnection((database, connection) =>
			{
				database.Server.Insert(new ServerDto
				{
					Id = "server1",
					Data = "",
					LastHeartbeat = database.GetServerTimeUtc().AddDays(-1)
				});
				database.Server.Insert(new ServerDto
				{
					Id = "server2",
					Data = "",
					LastHeartbeat = database.GetServerTimeUtc().AddHours(-12)
				});

				connection.RemoveTimedOutServers(TimeSpan.FromHours(15));

				var liveServer = database.Server.FindAll().Single();
				Assert.Equal("server2", liveServer.Id);
			});
		}

		[Fact, CleanDatabase]
		public void GetAllItemsFromSet_ThrowsAnException_WhenKeyIsNull()
		{
			UseConnection((database, connection) =>
				Assert.Throws<ArgumentNullException>(() => connection.GetAllItemsFromSet(null)));
		}

		[Fact, CleanDatabase]
		public void GetAllItemsFromSet_ReturnsEmptyCollection_WhenKeyDoesNotExist()
		{
			UseConnection((database, connection) =>
			{
				var result = connection.GetAllItemsFromSet("some-set");

				Assert.NotNull(result);
				Assert.Equal(0, result.Count);
			});
		}

		[Fact, CleanDatabase]
		public void GetAllItemsFromSet_ReturnsAllItems()
		{
			UseConnection((database, connection) =>
			{
				// Arrange
				database.Set.Insert(new SetDto
				{
					Id = ObjectId.GenerateNewId(),
					Key = "some-set",
					Score = 0.0,
					Value = "1"
				});
				database.Set.Insert(new SetDto
				{
					Id = ObjectId.GenerateNewId(),
					Key = "some-set",
					Score = 0.0,
					Value = "2"
				});
				database.Set.Insert(new SetDto
				{
					Id = ObjectId.GenerateNewId(),
					Key = "another-set",
					Score = 0.0,
					Value = "3"
				});

				// Act
				var result = connection.GetAllItemsFromSet("some-set");

				// Assert
				Assert.Equal(2, result.Count);
				Assert.Contains("1", result);
				Assert.Contains("2", result);
			});
		}

		[Fact, CleanDatabase]
		public void SetRangeInHash_ThrowsAnException_WhenKeyIsNull()
		{
			UseConnection((database, connection) =>
			{
				var exception = Assert.Throws<ArgumentNullException>(
					() => connection.SetRangeInHash(null, new Dictionary<string, string>()));

				Assert.Equal("key", exception.ParamName);
			});
		}

		[Fact, CleanDatabase]
		public void SetRangeInHash_ThrowsAnException_WhenKeyValuePairsArgumentIsNull()
		{
			UseConnection((database, connection) =>
			{
				var exception = Assert.Throws<ArgumentNullException>(
					() => connection.SetRangeInHash("some-hash", null));

				Assert.Equal("keyValuePairs", exception.ParamName);
			});
		}

		[Fact, CleanDatabase]
		public void SetRangeInHash_MergesAllRecords()
		{
			UseConnection((database, connection) =>
			{
				connection.SetRangeInHash("some-hash", new Dictionary<string, string>
						{
							{ "Key1", "Value1" },
							{ "Key2", "Value2" }
						});

				var result = database.Hash.Find(Query<HashDto>.EQ(_=>_.Key, "some-hash"))
					.ToDictionary(x => x.Field, x => x.Value);

				Assert.Equal("Value1", result["Key1"]);
				Assert.Equal("Value2", result["Key2"]);
			});
		}

		[Fact, CleanDatabase]
		public void GetAllEntriesFromHash_ThrowsAnException_WhenKeyIsNull()
		{
			UseConnection((database, connection) =>
				Assert.Throws<ArgumentNullException>(() => connection.GetAllEntriesFromHash(null)));
		}

		[Fact, CleanDatabase]
		public void GetAllEntriesFromHash_ReturnsNull_IfHashDoesNotExist()
		{
			UseConnection((database, connection) =>
			{
				var result = connection.GetAllEntriesFromHash("some-hash");
				Assert.Null(result);
			});
		}

		[Fact, CleanDatabase]
		public void GetAllEntriesFromHash_ReturnsAllKeysAndTheirValues()
		{
			UseConnection((database, connection) =>
			{
				// Arrange
				database.Hash.Insert(new HashDto
				{
					Id = ObjectId.GenerateNewId(),
					Key = "some-hash",
					Field = "Key1",
					Value = "Value1"
				});
				database.Hash.Insert(new HashDto
				{
					Id = ObjectId.GenerateNewId(),
					Key = "some-hash",
					Field = "Key2",
					Value = "Value2"
				});
				database.Hash.Insert(new HashDto
				{
					Id = ObjectId.GenerateNewId(),
					Key = "another-hash",
					Field = "Key3",
					Value = "Value3"
				});

				// Act
				var result = connection.GetAllEntriesFromHash("some-hash");

				// Assert
				Assert.NotNull(result);
				Assert.Equal(2, result.Count);
				Assert.Equal("Value1", result["Key1"]);
				Assert.Equal("Value2", result["Key2"]);
			});
		}

		private void UseConnection(Action<HangfireDbContext, MongoConnection> action)
		{
			using (var database = ConnectionUtils.CreateConnection())
			{
				using (var connection = new MongoConnection(database, _providers))
				{
					action(database, connection);
				}
			}
		}

		public static void SampleMethod(string arg)
		{
		}
	}
}