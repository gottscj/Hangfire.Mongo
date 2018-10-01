using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Hangfire.Common;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.Dto;
using Hangfire.Mongo.PersistentJobQueue;
using Hangfire.Mongo.Tests.Utils;
using Hangfire.Server;
using Hangfire.States;
using Hangfire.Storage;
using MongoDB.Bson;
using MongoDB.Driver;
using Moq;
using Xunit;

namespace Hangfire.Mongo.Tests
{
#pragma warning disable 1591
    [Collection("Database")]
    public class MongoConnectionFacts
    {
        private readonly Mock<IPersistentJobQueue> _queue;
        private readonly PersistentJobQueueProviderCollection _providers;

        public MongoConnectionFacts()
        {
            _queue = new Mock<IPersistentJobQueue>();

            var provider = new Mock<IPersistentJobQueueProvider>();
            provider.Setup(x => x.GetJobQueue(It.IsNotNull<HangfireDbContext>())).Returns(_queue.Object);

            _providers = new PersistentJobQueueProviderCollection(provider.Object);
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
                        DateTime.UtcNow,
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
                        Job.FromExpression(() => HangfireTestJobs.SampleMethod("hello")),
                        null,
                        DateTime.UtcNow,
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
                var jobId = connection.CreateExpiredJob(
                    Job.FromExpression(() => HangfireTestJobs.SampleMethod("Hello")),
                    new Dictionary<string, string> { { "Key1", "Value1" }, { "Key2", "Value2" } },
                    createdAt,
                    TimeSpan.FromDays(1));

                Assert.NotNull(jobId);
                Assert.NotEmpty(jobId);

                var databaseJob = database.JobGraph.OfType<JobDto>().Find(new BsonDocument()).ToList().Single();
                Assert.Equal(jobId, databaseJob.Id.ToString());
                Assert.Equal(createdAt, databaseJob.CreatedAt);
                Assert.Null(databaseJob.StateName);

                var invocationData = JobHelper.FromJson<InvocationData>(databaseJob.InvocationData);
                invocationData.Arguments = databaseJob.Arguments;

                var job = invocationData.Deserialize();
                Assert.Equal(typeof(HangfireTestJobs), job.Type);
                Assert.Equal(nameof(HangfireTestJobs.SampleMethod), job.Method.Name);
                Assert.Equal("Hello", job.Args[0]);

                Assert.True(createdAt.AddDays(1).AddMinutes(-1) < databaseJob.ExpireAt);
                Assert.True(databaseJob.ExpireAt < createdAt.AddDays(1).AddMinutes(1));

                var parameters = database
                    .JobGraph.OfType<JobDto>()
                    .Find(Builders<JobDto>.Filter.Eq(_ => _.Id, ObjectId.Parse(jobId)))
                    .Project(j => j.Parameters)
                    .ToList()
                    .SelectMany(j => j)
                    .ToDictionary(p => p.Key, x => x.Value);

                Assert.NotNull(parameters);
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
                var result = connection.GetJobData(ObjectId.GenerateNewId().ToString());
                Assert.Null(result);
            });
        }

        [Fact, CleanDatabase]
        public void GetJobData_ReturnsResult_WhenJobExists()
        {
            UseConnection((database, connection) =>
            {
                var job = Job.FromExpression(() => HangfireTestJobs.SampleMethod("wrong"));

                var jobDto = new JobDto
                {
                    Id = ObjectId.GenerateNewId(),
                    InvocationData = JobHelper.ToJson(InvocationData.Serialize(job)),
                    Arguments = "[\"\\\"Arguments\\\"\"]",
                    StateName = SucceededState.StateName,
                    CreatedAt = DateTime.UtcNow
                };
                database.JobGraph.InsertOne(jobDto);

                var result = connection.GetJobData(jobDto.Id.ToString());

                Assert.NotNull(result);
                Assert.NotNull(result.Job);
                Assert.Equal(SucceededState.StateName, result.State);
                Assert.Equal("Arguments", result.Job.Args[0]);
                Assert.Null(result.LoadException);
                Assert.True(DateTime.UtcNow.AddMinutes(-1) < result.CreatedAt);
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
                var result = connection.GetStateData(ObjectId.GenerateNewId().ToString());
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

                var state = new StateDto
                {
                    Name = "old-state",
                    CreatedAt = DateTime.UtcNow
                };
                var jobDto = new JobDto
                {
                    Id = ObjectId.GenerateNewId(),
                    InvocationData = "",
                    Arguments = "",
                    StateName = "",
                    CreatedAt = DateTime.UtcNow,
                    StateHistory = new[] { state }
                };

                database.JobGraph.InsertOne(jobDto);
                var jobId = jobDto.Id;

                var update = Builders<JobDto>
                    .Update
                    .Set(j => j.StateName, state.Name)
                    .Push(j => j.StateHistory, new StateDto
                    {
                        Name = "Name",
                        Reason = "Reason",
                        Data = data,
                        CreatedAt = DateTime.UtcNow
                    });

                database.JobGraph.OfType<JobDto>().UpdateOne(j => j.Id == jobId, update);

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
                    Id = ObjectId.GenerateNewId(),
                    InvocationData = JobHelper.ToJson(new InvocationData(null, null, null, null)),
                    Arguments = "[\"\\\"Arguments\\\"\"]",
                    StateName = SucceededState.StateName,
                    CreatedAt = DateTime.UtcNow
                };
                database.JobGraph.InsertOne(jobDto);
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
                    Id = ObjectId.GenerateNewId(),
                    InvocationData = "",
                    Arguments = "",
                    CreatedAt = DateTime.UtcNow
                };
                database.JobGraph.InsertOne(jobDto);
                var jobId = jobDto.Id;

                connection.SetJobParameter(jobId.ToString(), "Name", "Value");

                var parameters = database
                    .JobGraph.OfType<JobDto>()
                    .Find(j => j.Id == jobId)
                    .Project(j => j.Parameters)
                    .FirstOrDefault();

                Assert.NotNull(parameters);
                Assert.Equal("Value", parameters["Name"]);
            });
        }

        [Fact, CleanDatabase]
        public void SetParameter_UpdatesValue_WhenParameterWithTheGivenName_AlreadyExists()
        {
            UseConnection((database, connection) =>
            {
                var jobDto = new JobDto
                {
                    Id = ObjectId.GenerateNewId(),
                    InvocationData = "",
                    Arguments = "",
                    CreatedAt = DateTime.UtcNow
                };
                database.JobGraph.InsertOne(jobDto);
                var jobId = jobDto.Id;

                connection.SetJobParameter(jobId.ToString(), "Name", "Value");
                connection.SetJobParameter(jobId.ToString(), "Name", "AnotherValue");

                var parameters = database
                    .JobGraph.OfType<JobDto>()
                    .Find(j => j.Id == jobId)
                    .Project(j => j.Parameters)
                    .FirstOrDefault();

                Assert.NotNull(parameters);
                Assert.Equal("AnotherValue", parameters["Name"]);
            });
        }

        [Fact, CleanDatabase]
        public void SetParameter_CanAcceptNulls_AsValues()
        {
            UseConnection((database, connection) =>
            {
                var jobDto = new JobDto
                {
                    Id = ObjectId.GenerateNewId(),
                    InvocationData = "",
                    Arguments = "",
                    CreatedAt = DateTime.UtcNow
                };
                database.JobGraph.InsertOne(jobDto);
                var jobId = jobDto.Id;

                connection.SetJobParameter(jobId.ToString(), "Name", null);

                var parameters = database
                    .JobGraph.OfType<JobDto>()
                    .Find(j => j.Id == jobId)
                    .Project(j => j.Parameters)
                    .FirstOrDefault();

                Assert.NotNull(parameters);
                Assert.Null(parameters["Name"]);
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
                var value = connection.GetJobParameter(ObjectId.GenerateNewId().ToString(), "hello");
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
                    Id = ObjectId.GenerateNewId(),
                    InvocationData = "",
                    Arguments = "",
                    CreatedAt = DateTime.UtcNow
                };
                database.JobGraph.InsertOne(jobDto);


                connection.SetJobParameter(jobDto.Id.ToString(), "name", "value");

                var value = connection.GetJobParameter(jobDto.Id.ToString(), "name");

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
                database.JobGraph.InsertOne(new SetDto
                {
                    Id = ObjectId.GenerateNewId(),
                    Key = "key",
                    Score = 1.0,
                    Value = "1.0"
                });
                database.JobGraph.InsertOne(new SetDto
                {
                    Id = ObjectId.GenerateNewId(),
                    Key = "key",
                    Score = -1.0,
                    Value = "-1.0"
                });
                database.JobGraph.InsertOne(new SetDto
                {
                    Id = ObjectId.GenerateNewId(),
                    Key = "key",
                    Score = -5.0,
                    Value = "-5.0"
                });
                database.JobGraph.InsertOne(new SetDto
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

                var server = database.Server.Find(new BsonDocument()).Single();
                Assert.Equal("server", server.Id);
                Assert.True(server.Data.StartsWith("{\"WorkerCount\":4,\"Queues\":[\"critical\",\"default\"],\"StartedAt\":", StringComparison.Ordinal),
                    server.Data);
                Assert.NotNull(server.LastHeartbeat);

                var context2 = new ServerContext
                {
                    Queues = new[] { "default" },
                    WorkerCount = 1000
                };
                connection.AnnounceServer("server", context2);
                var sameServer = database.Server.Find(new BsonDocument()).Single();
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
                database.Server.InsertOne(new ServerDto
                {
                    Id = "Server1",
                    Data = "",
                    LastHeartbeat = DateTime.UtcNow
                });
                database.Server.InsertOne(new ServerDto
                {
                    Id = "Server2",
                    Data = "",
                    LastHeartbeat = DateTime.UtcNow
                });

                connection.RemoveServer("Server1");

                var server = database.Server.Find(new BsonDocument()).ToList().Single();
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
                database.Server.InsertOne(new ServerDto
                {
                    Id = "server1",
                    Data = "",
                    LastHeartbeat = new DateTime(2012, 12, 12, 12, 12, 12, DateTimeKind.Utc)
                });
                database.Server.InsertOne(new ServerDto
                {
                    Id = "server2",
                    Data = "",
                    LastHeartbeat = new DateTime(2012, 12, 12, 12, 12, 12, DateTimeKind.Utc)
                });

                connection.Heartbeat("server1");

                var servers = database.Server.Find(new BsonDocument()).ToList()
                    .ToDictionary(x => x.Id, x => x.LastHeartbeat);

                Assert.True(servers.ContainsKey("server1"));
                Assert.True(servers.ContainsKey("server2"));
                Assert.NotEqual(2012, servers["server1"].Value.Year);
                Assert.Equal(2012, servers["server2"].Value.Year);
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
                database.Server.InsertOne(new ServerDto
                {
                    Id = "server1",
                    Data = "",
                    LastHeartbeat = DateTime.UtcNow.AddDays(-1)
                });
                database.Server.InsertOne(new ServerDto
                {
                    Id = "server2",
                    Data = "",
                    LastHeartbeat = DateTime.UtcNow.AddHours(-12)
                });

                connection.RemoveTimedOutServers(TimeSpan.FromHours(15));

                var liveServer = database.Server.Find(new BsonDocument()).ToList().Single();
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
                Assert.Empty(result);
            });
        }

        [Fact, CleanDatabase]
        public void GetAllItemsFromSet_ReturnsAllItems_InCorrectOrder()
        {
            UseConnection((database, connection) =>
            {
                // Arrange
                database.JobGraph.InsertOne(new SetDto
                {
                    Id = ObjectId.GenerateNewId(),
                    Key = "some-set",
                    Score = 0.0,
                    Value = "1"
                });
                database.JobGraph.InsertOne(new SetDto
                {
                    Id = ObjectId.GenerateNewId(),
                    Key = "some-set",
                    Score = 0.0,
                    Value = "2"
                });
                database.JobGraph.InsertOne(new SetDto
                {
                    Id = ObjectId.GenerateNewId(),
                    Key = "another-set",
                    Score = 0.0,
                    Value = "3"
                });
                database.JobGraph.InsertOne(new SetDto
                {
                    Id = ObjectId.GenerateNewId(),
                    Key = "some-set",
                    Score = 0.0,
                    Value = "4"
                });
                database.JobGraph.InsertOne(new SetDto
                {
                    Id = ObjectId.GenerateNewId(),
                    Key = "some-set",
                    Score = 0.0,
                    Value = "5"
                });
                database.JobGraph.InsertOne(new SetDto
                {
                    Id = ObjectId.GenerateNewId(),
                    Key = "some-set",
                    Score = 0.0,
                    Value = "6"
                });
                // Act
                var result = connection.GetAllItemsFromSet("some-set");

                // Assert
                Assert.Equal(5, result.Count);
                Assert.Contains("1", result);
                Assert.Contains("2", result);
                Assert.Equal(new[] { "1", "2", "4", "5", "6" }, result);
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

                var result = database.JobGraph.OfType<HashDto>()
                    .Find(Builders<HashDto>.Filter.Eq(_ => _.Key, "some-hash"))
                    .First()
                    .Fields;

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
                database.JobGraph.InsertOne(new HashDto
                {
                    Id = ObjectId.GenerateNewId(),
                    Key = "some-hash",
                    Fields = new Dictionary<string, string>
                    {
                        ["Key1"] = "Value1",
                        ["Key2"] = "Value2",
                    },
                });
                database.JobGraph.InsertOne(new HashDto
                {
                    Id = ObjectId.GenerateNewId(),
                    Key = "another-hash",
                    Fields = new Dictionary<string, string>
                    {
                        ["Key3"] = "Value3"
                    },
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

        [Fact, CleanDatabase]
        public void GetSetCount_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection((database, connection) =>
            {
                Assert.Throws<ArgumentNullException>(
                    () => connection.GetSetCount(null));
            });
        }

        [Fact, CleanDatabase]
        public void GetSetCount_ReturnsZero_WhenSetDoesNotExist()
        {
            UseConnection((database, connection) =>
            {
                var result = connection.GetSetCount("my-set");
                Assert.Equal(0, result);
            });
        }

        [Fact, CleanDatabase]
        public void GetSetCount_ReturnsNumberOfElements_InASet()
        {
            UseConnection((database, connection) =>
            {
                database.JobGraph.InsertOne(new SetDto
                {
                    Id = ObjectId.GenerateNewId(),
                    Key = "set-1",
                    Value = "value-1"
                });
                database.JobGraph.InsertOne(new SetDto
                {
                    Id = ObjectId.GenerateNewId(),
                    Key = "set-2",
                    Value = "value-1"
                });
                database.JobGraph.InsertOne(new SetDto
                {
                    Id = ObjectId.GenerateNewId(),
                    Key = "set-1",
                    Value = "value-2"
                });

                var result = connection.GetSetCount("set-1");

                Assert.Equal(2, result);
            });
        }

        [Fact, CleanDatabase]
        public void GetRangeFromSet_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection((database, connection) =>
            {
                Assert.Throws<ArgumentNullException>(() => connection.GetRangeFromSet(null, 0, 1));
            });
        }

        [Fact, CleanDatabase]
        public void GetRangeFromSet_ReturnsPagedElementsInCorrectOrder()
        {
            UseConnection((database, connection) =>
            {
                database.JobGraph.InsertOne(new SetDto
                {
                    Id = ObjectId.GenerateNewId(),
                    Key = "set-1",
                    Value = "1",
                    Score = 0.0
                });

                database.JobGraph.InsertOne(new SetDto
                {
                    Id = ObjectId.GenerateNewId(),
                    Key = "set-1",
                    Value = "2",
                    Score = 0.0
                });

                database.JobGraph.InsertOne(new SetDto
                {
                    Id = ObjectId.GenerateNewId(),
                    Key = "set-1",
                    Value = "3",
                    Score = 0.0
                });

                database.JobGraph.InsertOne(new SetDto
                {
                    Id = ObjectId.GenerateNewId(),
                    Key = "set-1",
                    Value = "4",
                    Score = 0.0
                });

                database.JobGraph.InsertOne(new SetDto
                {
                    Id = ObjectId.GenerateNewId(),
                    Key = "set-2",
                    Value = "5",
                    Score = 0.0
                });

                database.JobGraph.InsertOne(new SetDto
                {
                    Id = ObjectId.GenerateNewId(),
                    Key = "set-1",
                    Value = "6",
                    Score = 0.0
                });

                var result = connection.GetRangeFromSet("set-1", 1, 8);

                Assert.Equal(new[] { "2", "3", "4", "6" }, result);
            });
        }

        [Fact, CleanDatabase]
        public void GetSetTtl_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection((database, connection) =>
            {
                Assert.Throws<ArgumentNullException>(() => connection.GetSetTtl(null));
            });
        }

        [Fact, CleanDatabase]
        public void GetSetTtl_ReturnsNegativeValue_WhenSetDoesNotExist()
        {
            UseConnection((database, connection) =>
            {
                var result = connection.GetSetTtl("my-set");
                Assert.True(result < TimeSpan.Zero);
            });
        }

        [Fact, CleanDatabase]
        public void GetSetTtl_ReturnsExpirationTime_OfAGivenSet()
        {
            UseConnection((database, connection) =>
            {
                // Arrange
                database.JobGraph.InsertOne(new SetDto
                {
                    Id = ObjectId.GenerateNewId(),
                    Key = "set-1",
                    Value = "1",
                    Score = 0.0,
                    ExpireAt = DateTime.UtcNow.AddMinutes(60)
                });

                database.JobGraph.InsertOne(new SetDto
                {
                    Id = ObjectId.GenerateNewId(),
                    Key = "set-2",
                    Value = "2",
                    Score = 0.0,
                    ExpireAt = null
                });

                // Act
                var result = connection.GetSetTtl("set-1");

                // Assert
                Assert.True(TimeSpan.FromMinutes(59) < result);
                Assert.True(result < TimeSpan.FromMinutes(61));
            });
        }

        [Fact, CleanDatabase]
        public void GetCounter_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection((database, connection) =>
            {
                Assert.Throws<ArgumentNullException>(
                    () => connection.GetCounter(null));
            });
        }

        [Fact, CleanDatabase]
        public void GetCounter_ReturnsZero_WhenKeyDoesNotExist()
        {
            UseConnection((database, connection) =>
            {
                var result = connection.GetCounter("my-counter");
                Assert.Equal(0, result);
            });
        }

        [Fact, CleanDatabase]
        public void GetCounter_ReturnsSumOfValues_InCounterTable()
        {
            UseConnection((database, connection) =>
            {
                // Arrange
                database.JobGraph.InsertOne(new CounterDto
                {
                    Id = ObjectId.GenerateNewId(),
                    Key = "counter-1",
                    Value = 2L
                });
                database.JobGraph.InsertOne(new CounterDto
                {
                    Id = ObjectId.GenerateNewId(),
                    Key = "counter-2",
                    Value = 1L
                });
                
                // Act
                var result = connection.GetCounter("counter-1");

                // Assert
                Assert.Equal(2, result);
            });
        }

        [Fact, CleanDatabase]
        public void GetHashCount_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection((database, connection) =>
            {
                Assert.Throws<ArgumentNullException>(() => connection.GetHashCount(null));
            });
        }

        [Fact, CleanDatabase]
        public void GetHashCount_ReturnsZero_WhenKeyDoesNotExist()
        {
            UseConnection((database, connection) =>
            {
                var result = connection.GetHashCount("my-hash");
                Assert.Equal(0, result);
            });
        }

        [Fact, CleanDatabase]
        public void GetHashCount_ReturnsNumber_OfHashFields()
        {
            UseConnection((database, connection) =>
            {
                // Arrange
                database.JobGraph.InsertOne(new HashDto
                {
                    Id = ObjectId.GenerateNewId(),
                    Fields = new Dictionary<string, string>
                    {
                        ["field-1"] = "field-1-value",
                        ["field-2"] = "field-2-value",
                        
                    },
                    Key = "hash-1",
                });
                database.JobGraph.InsertOne(new HashDto
                {
                    Id = ObjectId.GenerateNewId(),
                    Key = "hash-2",
                    Fields = new Dictionary<string, string>
                    {
                        ["field-1"] = "field-1-value",
                        
                    },
                });

                // Act
                var result = connection.GetHashCount("hash-1");

                // Assert
                Assert.Equal(2, result);
            });
        }

        [Fact, CleanDatabase]
        public void GetHashTtl_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection((database, connection) =>
            {
                Assert.Throws<ArgumentNullException>(
                    () => connection.GetHashTtl(null));
            });
        }

        [Fact, CleanDatabase]
        public void GetHashTtl_ReturnsNegativeValue_WhenHashDoesNotExist()
        {
            UseConnection((database, connection) =>
            {
                var result = connection.GetHashTtl("my-hash");
                Assert.True(result < TimeSpan.Zero);
            });
        }

        [Fact, CleanDatabase]
        public void GetHashTtl_ReturnsExpirationTimeForHash()
        {
            UseConnection((database, connection) =>
            {
                // Arrange
                database.JobGraph.InsertOne(new HashDto
                {
                    Id = ObjectId.GenerateNewId(),
                    Key = "hash-1",
                    Fields = new Dictionary<string, string>
                    {
                        ["field-1"] = "field-1-value",
                        
                    },
                    ExpireAt = DateTime.UtcNow.AddHours(1)
                });
                database.JobGraph.InsertOne(new HashDto
                {
                    Id = ObjectId.GenerateNewId(),
                    Key = "hash-2",
                    Fields = new Dictionary<string, string>
                    {
                        ["field-1"] = "field-1-value",
                        
                    },
                    ExpireAt = null
                });

                // Act
                var result = connection.GetHashTtl("hash-1");

                // Assert
                Assert.True(TimeSpan.FromMinutes(59) < result);
                Assert.True(result < TimeSpan.FromMinutes(61));
            });
        }

        [Fact, CleanDatabase]
        public void GetValueFromHash_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection((database, connection) =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.GetValueFromHash(null, "name"));

                Assert.Equal("key", exception.ParamName);
            });
        }

        [Fact, CleanDatabase]
        public void GetValueFromHash_ThrowsAnException_WhenNameIsNull()
        {
            UseConnection((database, connection) =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.GetValueFromHash("key", null));

                Assert.Equal("name", exception.ParamName);
            });
        }

        [Fact, CleanDatabase]
        public void GetValueFromHash_ReturnsNull_WhenHashDoesNotExist()
        {
            UseConnection((database, connection) =>
            {
                var result = connection.GetValueFromHash("my-hash", "name");
                Assert.Null(result);
            });
        }

        [Fact, CleanDatabase]
        public void GetValueFromHash_ReturnsValue_OfAGivenField()
        {
            UseConnection((database, connection) =>
            {
                // Arrange
                database.JobGraph.InsertOne(new HashDto
                {
                    Id = ObjectId.GenerateNewId(),
                    Key = "hash-1",
                    Fields = new Dictionary<string, string>
                    {
                        ["field-1"] = "1",
                        ["field-2"] = "2",
                    },
                });
                database.JobGraph.InsertOne(new HashDto
                {
                    Id = ObjectId.GenerateNewId(),
                    Key = "hash-2",
                    Fields = new Dictionary<string, string>
                    {
                        ["field-1"] = "2"
                    },
                });

                // Act
                var result = connection.GetValueFromHash("hash-1", "field-1");

                // Assert
                Assert.Equal("1", result);
            });
        }

        [Fact, CleanDatabase]
        public void GetListCount_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection((database, connection) =>
            {
                Assert.Throws<ArgumentNullException>(
                    () => connection.GetListCount(null));
            });
        }

        [Fact, CleanDatabase]
        public void GetListCount_ReturnsZero_WhenListDoesNotExist()
        {
            UseConnection((database, connection) =>
            {
                var result = connection.GetListCount("my-list");
                Assert.Equal(0, result);
            });
        }

        [Fact, CleanDatabase]
        public void GetListCount_ReturnsTheNumberOfListElements()
        {
            UseConnection((database, connection) =>
            {
                // Arrange
                database.JobGraph.InsertOne(new ListDto
                {
                    Id = ObjectId.GenerateNewId(),
                    Key = "list-1",
                });
                database.JobGraph.InsertOne(new ListDto
                {
                    Id = ObjectId.GenerateNewId(),
                    Key = "list-1",
                });
                database.JobGraph.InsertOne(new ListDto
                {
                    Id = ObjectId.GenerateNewId(),
                    Key = "list-2",
                });

                // Act
                var result = connection.GetListCount("list-1");

                // Assert
                Assert.Equal(2, result);
            });
        }

        [Fact, CleanDatabase]
        public void GetListTtl_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection((database, connection) =>
            {
                Assert.Throws<ArgumentNullException>(
                    () => connection.GetListTtl(null));
            });
        }

        [Fact, CleanDatabase]
        public void GetListTtl_ReturnsNegativeValue_WhenListDoesNotExist()
        {
            UseConnection((database, connection) =>
            {
                var result = connection.GetListTtl("my-list");
                Assert.True(result < TimeSpan.Zero);
            });
        }

        [Fact, CleanDatabase]
        public void GetListTtl_ReturnsExpirationTimeForList()
        {
            UseConnection((database, connection) =>
            {
                // Arrange
                database.JobGraph.InsertOne(new ListDto
                {
                    Id = ObjectId.GenerateNewId(),
                    Key = "list-1",
                    ExpireAt = DateTime.UtcNow.AddHours(1)
                });
                database.JobGraph.InsertOne(new ListDto
                {
                    Id = ObjectId.GenerateNewId(),
                    Key = "list-2",
                    ExpireAt = null
                });

                // Act
                var result = connection.GetListTtl("list-1");

                // Assert
                Assert.True(TimeSpan.FromMinutes(59) < result);
                Assert.True(result < TimeSpan.FromMinutes(61));
            });
        }

        [Fact, CleanDatabase]
        public void GetRangeFromList_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection((database, connection) =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => connection.GetRangeFromList(null, 0, 1));

                Assert.Equal("key", exception.ParamName);
            });
        }

        [Fact, CleanDatabase]
        public void GetRangeFromList_ReturnsAnEmptyList_WhenListDoesNotExist()
        {
            UseConnection((database, connection) =>
            {
                var result = connection.GetRangeFromList("my-list", 0, 1);
                Assert.Empty(result);
            });
        }

        [Fact, CleanDatabase]
        public void GetRangeFromList_ReturnsAllEntries_WithinGivenBounds()
        {
            UseConnection((database, connection) =>
            {
                // Arrange
                database.JobGraph.InsertOne(new ListDto
                {
                    Id = ObjectId.GenerateNewId(),
                    Key = "list-1",
                    Value = "1"
                });
                database.JobGraph.InsertOne(new ListDto
                {
                    Id = ObjectId.GenerateNewId(),
                    Key = "list-2",
                    Value = "2"
                });
                database.JobGraph.InsertOne(new ListDto
                {
                    Id = ObjectId.GenerateNewId(),
                    Key = "list-1",
                    Value = "3"
                });
                database.JobGraph.InsertOne(new ListDto
                {
                    Id = ObjectId.GenerateNewId(),
                    Key = "list-1",
                    Value = "4"
                });
                database.JobGraph.InsertOne(new ListDto
                {
                    Id = ObjectId.GenerateNewId(),
                    Key = "list-1",
                    Value = "5"
                });

                // Act
                var result = connection.GetRangeFromList("list-1", 1, 2);

                // Assert
                Assert.Equal(new[] { "4", "3" }, result);
            });
        }

        [Fact, CleanDatabase]
        public void GetRangeFromList_ReturnsAllEntriesInCorrectOrder()
        {
            UseConnection((database, connection) =>
            {
                // Arrange
                var listDtos = new List<ListDto>
                {
                    new ListDto
                    {
                        Id = ObjectId.GenerateNewId(),
                        Key = "list-1",
                        Value = "1"
                    },
                    new ListDto
                    {
                        Id = ObjectId.GenerateNewId(),
                        Key = "list-1",
                        Value = "2"
                    },
                    new ListDto
                    {
                        Id = ObjectId.GenerateNewId(),
                        Key = "list-1",
                        Value = "3"
                    },
                    new ListDto
                    {
                        Id = ObjectId.GenerateNewId(),
                        Key = "list-1",
                        Value = "4"
                    },
                    new ListDto
                    {
                        Id = ObjectId.GenerateNewId(),
                        Key = "list-1",
                        Value = "5"
                    }
                };
                database.JobGraph.InsertMany(listDtos);

                // Act
                var result = connection.GetRangeFromList("list-1", 1, 5);

                // Assert
                Assert.Equal(new[] { "4", "3", "2", "1" }, result);
            });
        }

        [Fact, CleanDatabase]
        public void GetAllItemsFromList_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection((database, connection) =>
            {
                Assert.Throws<ArgumentNullException>(
                    () => connection.GetAllItemsFromList(null));
            });
        }

        [Fact, CleanDatabase]
        public void GetAllItemsFromList_ReturnsAnEmptyList_WhenListDoesNotExist()
        {
            UseConnection((database, connection) =>
            {
                var result = connection.GetAllItemsFromList("my-list");
                Assert.Empty(result);
            });
        }

        [Fact, CleanDatabase]
        public void GetAllItemsFromList_ReturnsAllItemsFromAGivenList_InCorrectOrder()
        {
            UseConnection((database, connection) =>
            {
                // Arrange
                database.JobGraph.InsertOne(new ListDto
                {
                    Id = ObjectId.GenerateNewId(),
                    Key = "list-1",
                    Value = "1"
                });
                database.JobGraph.InsertOne(new ListDto
                {
                    Id = ObjectId.GenerateNewId(),
                    Key = "list-2",
                    Value = "2"
                });
                database.JobGraph.InsertOne(new ListDto
                {
                    Id = ObjectId.GenerateNewId(),
                    Key = "list-1",
                    Value = "3"
                });
                database.JobGraph.InsertOne(new ListDto
                {
                    Id = ObjectId.GenerateNewId(),
                    Key = "list-1",
                    Value = "4"
                });
                database.JobGraph.InsertOne(new ListDto
                {
                    Id = ObjectId.GenerateNewId(),
                    Key = "list-1",
                    Value = "5"
                });

                // Act
                var result = connection.GetAllItemsFromList("list-1");

                // Assert
                Assert.Equal(new[] { "5", "4", "3", "1" }, result);
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

    }

#pragma warning restore 1591
}