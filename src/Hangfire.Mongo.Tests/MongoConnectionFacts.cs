using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Hangfire.Common;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.Dto;
using Hangfire.Mongo.Tests.Utils;
using Hangfire.Server;
using Hangfire.States;
using Hangfire.Storage;
using MongoDB.Bson;
using MongoDB.Driver;
using NSubstitute;
using Xunit;

namespace Hangfire.Mongo.Tests
{
#pragma warning disable 1591
    [Collection("Database")]
    public class MongoConnectionFacts
    {
        private readonly HangfireDbContext _dbContext;
        private readonly MongoConnection _connection;
		private readonly IJobQueueSemaphore _jobQueueSemaphoreMock;

        public MongoConnectionFacts(MongoIntegrationTestFixture fixture)
        {
            _jobQueueSemaphoreMock = Substitute.For<IJobQueueSemaphore>();
            var storageOptions = new MongoStorageOptions
            {
                Factory =
                {
                    JobQueueSemaphore = _jobQueueSemaphoreMock
                }
            };

            fixture.CleanDatabase();
            _dbContext = fixture.CreateDbContext();
            _connection = new MongoConnection(_dbContext, storageOptions);
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenConnectionIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new MongoConnection(null, null));

            Assert.Equal("database", exception.ParamName);
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenProvidersCollectionIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new MongoConnection(_dbContext, null));

            Assert.Equal("storageOptions", exception.ParamName);
        }

        [Fact]
        public void FetchNextJob_DelegatesItsExecution_ToTheQueue()
        {
            var token = new CancellationToken();
            var queues = new[] { "default" };
            var jobDto = new JobDto
            {
                Id = ObjectId.GenerateNewId(),
                Queue = "default",
                FetchedAt = null
            };
            _jobQueueSemaphoreMock.WaitNonBlock("default").Returns(true);
            var serializedJob = jobDto.Serialize();
            _dbContext.JobGraph.InsertOne(serializedJob);

            var fetchedJob = _connection.FetchNextJob(queues, token);

            Assert.Equal(fetchedJob.JobId, jobDto.Id.ToString());

            _jobQueueSemaphoreMock.Received(1).WaitNonBlock("default");
        }

        [Fact]
        public void CreateWriteTransaction_ReturnsNonNullInstance()
        {
            var transaction = _connection.CreateWriteTransaction();
            Assert.NotNull(transaction);
        }

        [Fact]
        public void AcquireLock_ReturnsNonNullInstance()
        {
            var @lock = _connection.AcquireDistributedLock("1", TimeSpan.FromSeconds(1));
            Assert.NotNull(@lock);
        }

        [Fact]
        public void CreateExpiredJob_ThrowsAnException_WhenJobIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => _connection.CreateExpiredJob(
                    null,
                    new Dictionary<string, string>(),
                    DateTime.UtcNow,
                    TimeSpan.Zero));

            Assert.Equal("job", exception.ParamName);
        }

        [Fact]
        public void CreateExpiredJob_ThrowsAnException_WhenParametersCollectionIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => _connection.CreateExpiredJob(
                    Job.FromExpression(() => HangfireTestJobs.SampleMethod("hello")),
                    null,
                    DateTime.UtcNow,
                    TimeSpan.Zero));

            Assert.Equal("parameters", exception.ParamName);
        }

        [Fact]
        public void CreateExpiredJob_CreatesAJobInTheStorage_AndSetsItsParameters()
        {
            var createdAt = new DateTime(2012, 12, 12, 0, 0, 0, 0, DateTimeKind.Utc);
            var jobId = _connection.CreateExpiredJob(
                Job.FromExpression(() => HangfireTestJobs.SampleMethod("Hello")),
                new Dictionary<string, string> { { "Key1", "Value1" }, { "Key2", "Value2" } },
                createdAt,
                TimeSpan.FromDays(1));

            Assert.NotNull(jobId);
            Assert.NotEmpty(jobId);

            var document = _dbContext.JobGraph
                .Find(new BsonDocument("_t", nameof(JobDto)))
                .Single();
            var databaseJob = new JobDto(document);
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

            var parameters = _dbContext
                .JobGraph
                .Find(new BsonDocument("_t", nameof(JobDto)) { ["_id"] = ObjectId.Parse(jobId) })
                .ToList()
                .SelectMany(j => new JobDto(j).Parameters)
                .ToDictionary(p => p.Key, x => x.Value);

            Assert.NotNull(parameters);
            Assert.Equal("Value1", parameters["Key1"]);
            Assert.Equal("Value2", parameters["Key2"]);
        }

        [Fact]
        public void GetJobData_ThrowsAnException_WhenJobIdIsNull()
        {
            Assert.Throws<ArgumentNullException>(() => _connection.GetJobData(null));
        }

        [Fact]
        public void GetJobData_ReturnsNull_WhenThereIsNoSuchJob()
        {
            var result = _connection.GetJobData(ObjectId.GenerateNewId().ToString());
            Assert.Null(result);
        }

        [Fact]
        public void GetJobData_ReturnsResult_WhenJobExists()
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
            _dbContext.JobGraph.InsertOne(jobDto.Serialize());

            var result = _connection.GetJobData(jobDto.Id.ToString());

            Assert.NotNull(result);
            Assert.NotNull(result.Job);
            Assert.Equal(SucceededState.StateName, result.State);
            Assert.Equal("Arguments", result.Job.Args[0]);
            Assert.Null(result.LoadException);
            Assert.True(DateTime.UtcNow.AddMinutes(-1) < result.CreatedAt);
            Assert.True(result.CreatedAt < DateTime.UtcNow.AddMinutes(1));
        }

        [Fact]
        public void GetStateData_ThrowsAnException_WhenJobIdIsNull()
        {
            Assert.Throws<ArgumentNullException>(() => _connection.GetStateData(null));
        }

        [Fact]
        public void GetStateData_ReturnsNull_IfThereIsNoSuchState()
        {
            var result = _connection.GetStateData(ObjectId.GenerateNewId().ToString());
            Assert.Null(result);
        }

        [Fact]
        public void GetStateData_ReturnsCorrectData()
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

            _dbContext.JobGraph.InsertOne(jobDto.Serialize());
            var jobId = jobDto.Id;

            var update = new BsonDocument
            {
                ["$set"] = new BsonDocument
                {
                    [nameof(JobDto.StateName)] = state.Name
                },
                ["$push"] = new BsonDocument
                {
                    [nameof(JobDto.StateHistory)] = new StateDto
                    {
                        Name = "Name",
                        Reason = "Reason",
                        Data = data,
                        CreatedAt = DateTime.UtcNow
                    }.Serialize()
                }
            };

            var filter = new BsonDocument
            {
                ["_id"] = jobId,
                ["_t"] = nameof(JobDto),
            };

            _dbContext.JobGraph.UpdateOne(filter, update);

            var result = _connection.GetStateData(jobId.ToString());
            Assert.NotNull(result);

            Assert.Equal("Name", result.Name);
            Assert.Equal("Reason", result.Reason);
            Assert.Equal("Value", result.Data["Key"]);
        }

        [Fact]
        public void GetJobData_ReturnsJobLoadException_IfThereWasADeserializationException()
        {
            var jobDto = new JobDto
            {
                Id = ObjectId.GenerateNewId(),
                InvocationData = JobHelper.ToJson(new InvocationData(null, null, null, null)),
                Arguments = "[\"\\\"Arguments\\\"\"]",
                StateName = SucceededState.StateName,
                CreatedAt = DateTime.UtcNow
            };
            _dbContext.JobGraph.InsertOne(jobDto.Serialize());
            var jobId = jobDto.Id;

            var result = _connection.GetJobData(jobId.ToString());

            Assert.NotNull(result.LoadException);
        }

        [Fact]
        public void SetParameter_ThrowsAnException_WhenJobIdIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(() => _connection.SetJobParameter(null, "name", "value"));

            Assert.Equal("id", exception.ParamName);
        }

        [Fact]
        public void SetParameter_ThrowsAnException_WhenNameIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => _connection.SetJobParameter("547527b4c6b6cc26a02d021d", null, "value"));

            Assert.Equal("name", exception.ParamName);
        }

        [Fact]
        public void SetParameters_CreatesNewParameter_WhenParameterWithTheGivenNameDoesNotExists()
        {
            var jobDto = new JobDto
            {
                Id = ObjectId.GenerateNewId(),
                InvocationData = "",
                Arguments = "",
                CreatedAt = DateTime.UtcNow
            };
            _dbContext.JobGraph.InsertOne(jobDto.Serialize());
            var jobId = jobDto.Id;

            _connection.SetJobParameter(jobId.ToString(), "Name", "Value");
            var filter = new BsonDocument
            {
                ["_id"] = jobId,
                ["_t"] = nameof(JobDto),
            };
            var job = new JobDto(_dbContext.JobGraph.Find(filter).First());

            Assert.NotNull(job.Parameters);
            Assert.Equal("Value", job.Parameters["Name"]);
        }

        [Fact]
        public void SetParameter_UpdatesValue_WhenParameterWithTheGivenName_AlreadyExists()
        {
            var jobDto = new JobDto
            {
                Id = ObjectId.GenerateNewId(),
                InvocationData = "",
                Arguments = "",
                CreatedAt = DateTime.UtcNow
            };
            _dbContext.JobGraph.InsertOne(jobDto.Serialize());
            var jobId = jobDto.Id;

            _connection.SetJobParameter(jobId.ToString(), "Name", "Value");
            _connection.SetJobParameter(jobId.ToString(), "Name", "AnotherValue");

            var filter = new BsonDocument
            {
                ["_id"] = jobId,
                ["_t"] = nameof(JobDto),
            };
            var job = new JobDto(_dbContext.JobGraph.Find(filter).FirstOrDefault());

            Assert.NotNull(job.Parameters);
            Assert.Equal("AnotherValue", job.Parameters["Name"]);
        }

        [Fact]
        public void SetParameter_CanAcceptNulls_AsValues()
        {
            var jobDto = new JobDto
            {
                Id = ObjectId.GenerateNewId(),
                InvocationData = "",
                Arguments = "",
                CreatedAt = DateTime.UtcNow
            };
            _dbContext.JobGraph.InsertOne(jobDto.Serialize());
            var jobId = jobDto.Id;

            _connection.SetJobParameter(jobId.ToString(), "Name", null);

            var filter = new BsonDocument
            {
                ["_id"] = jobId,
                ["_t"] = nameof(JobDto),
            };
            var job = new JobDto(_dbContext.JobGraph.Find(filter).FirstOrDefault());

            Assert.NotNull(job.Parameters);
            Assert.Null(job.Parameters["Name"]);
        }

        [Fact]
        public void GetParameter_ThrowsAnException_WhenJobIdIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => _connection.GetJobParameter(null, "hello"));

            Assert.Equal("id", exception.ParamName);
        }

        [Fact]
        public void GetParameter_ThrowsAnException_WhenNameIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => _connection.GetJobParameter("547527b4c6b6cc26a02d021d", null));

            Assert.Equal("name", exception.ParamName);
        }

        [Fact]
        public void GetParameter_ReturnsNull_WhenParameterDoesNotExists()
        {
            var value = _connection.GetJobParameter(ObjectId.GenerateNewId().ToString(), "hello");
            Assert.Null(value);
        }

        [Fact]
        public void GetParameter_ReturnsParameterValue_WhenJobExists()
        {
            var jobDto = new JobDto
            {
                Id = ObjectId.GenerateNewId(),
                InvocationData = "",
                Arguments = "",
                CreatedAt = DateTime.UtcNow
            };
            _dbContext.JobGraph.InsertOne(jobDto.Serialize());


            _connection.SetJobParameter(jobDto.Id.ToString(), "name", "value");

            var value = _connection.GetJobParameter(jobDto.Id.ToString(), "name");

            Assert.Equal("value", value);
        }

        [Fact]
        public void GetFirstByLowestScoreFromSet_ThrowsAnException_WhenKeyIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => _connection.GetFirstByLowestScoreFromSet(null, 0, 1));

            Assert.Equal("key", exception.ParamName);
        }

        [Fact]
        public void GetFirstByLowestScoreFromSet_ThrowsAnException_ToScoreIsLowerThanFromScore()
        {
            Assert.Throws<ArgumentException>(
                () => _connection.GetFirstByLowestScoreFromSet("key", 0, -1));
        }

        [Fact]
        public void GetFirstByLowestScoreFromSet_ReturnsNull_WhenTheKeyDoesNotExist()
        {
            var result = _connection.GetFirstByLowestScoreFromSet(
                "key", 0, 1);

            Assert.Null(result);
        }

        [Fact]
        public void GetFirstByLowestScoreFromSet_ReturnsTheValueWithTheLowestScore()
        {
            _dbContext.JobGraph.InsertOne(new SetDto
            {
                Id = ObjectId.GenerateNewId(),
                Key = "key<1.0>",
                Value = "1.0",
                Score = 1.0,
                SetType = "key"
            }.Serialize());
            _dbContext.JobGraph.InsertOne(new SetDto
            {
                Id = ObjectId.GenerateNewId(),
                Key = "key<-1.0>",
                Value = "-1.0",
                Score = -1.0,
                SetType = "key"
            }.Serialize());
            _dbContext.JobGraph.InsertOne(new SetDto
            {
                Id = ObjectId.GenerateNewId(),
                Key = "key<-5.0>",
                Value = "-5.0",
                Score = -5.0,
                SetType = "key"
            }.Serialize());
            _dbContext.JobGraph.InsertOne(new SetDto
            {
                Id = ObjectId.GenerateNewId(),
                Key = "another-key<-2.0>",
                Value = "-2.0",
                Score = -2.0,
                SetType = "another-key"
            }.Serialize());

            var result = _connection.GetFirstByLowestScoreFromSet("key", -1.0, 3.0);

            Assert.Equal("-1.0", result);
        }

        [Fact]
        public void GetFirstByLowestScoreFromSet_ReturnsTheValue_WhenKeyContainsRegexSpecialChars()
        {
            var key = "some+-[regex]?-#set";

            _dbContext.JobGraph.InsertOne(new SetDto
            {
                Id = ObjectId.GenerateNewId(),
                Key = $"{key}<1.0>",
                Value = "1.0",
                Score = 1.0,
                SetType = key
            }.Serialize());
            _dbContext.JobGraph.InsertOne(new SetDto
            {
                Id = ObjectId.GenerateNewId(),
                Key = $"{key}<-1.0>",
                Value = "-1.0",
                Score = -1.0,
                SetType = key
            }.Serialize());

            var result = _connection.GetFirstByLowestScoreFromSet(key, -1.0, 3.0);

            Assert.Equal("-1.0", result);
        }

        [Fact]
        public void AnnounceServer_ThrowsAnException_WhenServerIdIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => _connection.AnnounceServer(null, new ServerContext()));

            Assert.Equal("serverId", exception.ParamName);
        }

        [Fact]
        public void AnnounceServer_ThrowsAnException_WhenContextIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => _connection.AnnounceServer("server", null));

            Assert.Equal("context", exception.ParamName);
        }

        [Fact]
        public void AnnounceServer_CreatesOrUpdatesARecord()
        {
            var context1 = new ServerContext
            {
                Queues = new[] { "critical", "default" },
                WorkerCount = 4
            };
            _connection.AnnounceServer("server", context1);

            var server = new ServerDto(_dbContext.Server.AsQueryable().Single());
            Assert.Equal("server", server.Id);
            Assert.Equal(context1.WorkerCount, server.WorkerCount);
            Assert.Equal(context1.Queues, server.Queues);
            Assert.NotNull(server.StartedAt);
            Assert.NotNull(server.LastHeartbeat);

            var context2 = new ServerContext
            {
                Queues = new[] { "default" },
                WorkerCount = 1000
            };
            _connection.AnnounceServer("server", context2);
            var sameServer =  new ServerDto(_dbContext.Server.AsQueryable().Single());
            Assert.Equal("server", sameServer.Id);
            Assert.Equal(context2.WorkerCount, sameServer.WorkerCount);
        }

        [Fact]
        public void RemoveServer_ThrowsAnException_WhenServerIdIsNull()
        {
            Assert.Throws<ArgumentNullException>(() => _connection.RemoveServer(null));
        }

        [Fact]
        public void RemoveServer_RemovesAServerRecord()
        {
            _dbContext.Server.InsertOne(new ServerDto
            {
                Id = "Server1",
                LastHeartbeat = DateTime.UtcNow
            }.Serialize());
            _dbContext.Server.InsertOne(new ServerDto
            {
                Id = "Server2",
                LastHeartbeat = DateTime.UtcNow
            }.Serialize());

            _connection.RemoveServer("Server1");

            var server = new ServerDto(_dbContext.Server.AsQueryable().Single());
            Assert.NotEqual("Server1", server.Id, StringComparer.OrdinalIgnoreCase);
        }

        [Fact]
        public void Heartbeat_ThrowsAnException_WhenServerIdIsNull()
        {
            Assert.Throws<ArgumentNullException>(
                () => _connection.Heartbeat(null));
        }

        [Fact]
        public void Heartbeat_ThrowsBackgroundServerGoneException_WhenGivenServerDoesNotExist()
        {
            var serverId = Guid.NewGuid().ToString();

            Assert.Throws<BackgroundServerGoneException>(
                () => _connection.Heartbeat(serverId));
        }

        [Fact]
        public void Heartbeat_UpdatesLastHeartbeat_OfTheServerWithGivenId()
        {
            _dbContext.Server.InsertOne(new ServerDto
            {
                Id = "server1",
                LastHeartbeat = new DateTime(2012, 12, 12, 12, 12, 12, DateTimeKind.Utc)
            }.Serialize());
            _dbContext.Server.InsertOne(new ServerDto
            {
                Id = "server2",
                LastHeartbeat = new DateTime(2012, 12, 12, 12, 12, 12, DateTimeKind.Utc)
            }.Serialize());

            _connection.Heartbeat("server1");

            var servers = _dbContext.Server.AsQueryable().ToList().Select(b => new ServerDto(b))
                .ToDictionary(x => x.Id, x => x.LastHeartbeat);

            Assert.True(servers.ContainsKey("server1"));
            Assert.True(servers.ContainsKey("server2"));
            Assert.NotEqual(2012, servers["server1"].Value.Year);
            Assert.Equal(2012, servers["server2"].Value.Year);
        }

        [Fact]
        public void RemoveTimedOutServers_ThrowsAnException_WhenTimeOutIsNegative()
        {
            Assert.Throws<ArgumentException>(
                () => _connection.RemoveTimedOutServers(TimeSpan.FromMinutes(-5)));
        }

        [Fact]
        public void RemoveTimedOutServers_DoItsWorkPerfectly()
        {
            var id1 = ObjectId.GenerateNewId();
            var id2 = ObjectId.GenerateNewId();

            var id2Newest = id2 > id1;
            _dbContext.Server.InsertOne(new ServerDto
            {
                Id = "server1",
                LastHeartbeat = DateTime.UtcNow.AddDays(-1)
            }.Serialize());
            _dbContext.Server.InsertOne(new ServerDto
            {
                Id = "server2",
                LastHeartbeat = DateTime.UtcNow.AddHours(-12)
            }.Serialize());

            _connection.RemoveTimedOutServers(TimeSpan.FromHours(15));

            var liveServer = new ServerDto(_dbContext.Server.AsQueryable().Single());
            Assert.Equal("server2", liveServer.Id);
        }

        [Fact]
        public void GetAllItemsFromSet_ThrowsAnException_WhenKeyIsNull()
        {
            Assert.Throws<ArgumentNullException>(() => _connection.GetAllItemsFromSet(null));
        }

        [Fact]
        public void GetAllItemsFromSet_ReturnsEmptyCollection_WhenKeyDoesNotExist()
        {
            var result = _connection.GetAllItemsFromSet("some-set");

            Assert.NotNull(result);
            Assert.Empty(result);
        }

        [Fact]
        public void GetAllItemsFromSet_ReturnsAllItems_InCorrectOrder()
        {
            // Arrange
            _dbContext.JobGraph.InsertOne(new SetDto
            {
                Id = ObjectId.GenerateNewId(),
                Key = "some-set<1>",
                Value = "1",
                Score = 0.0,
                SetType = "some-set"
            }.Serialize());
            _dbContext.JobGraph.InsertOne(new SetDto
            {
                Id = ObjectId.GenerateNewId(),
                Key = "some-set<2>",
                Value = "2",
                Score = 0.0,
                SetType = "some-set"
            }.Serialize());
            _dbContext.JobGraph.InsertOne(new SetDto
            {
                Id = ObjectId.GenerateNewId(),
                Key = "another-set<3>",
                Value = "3",
                Score = 0.0,
                SetType = "another-set"
            }.Serialize());
            _dbContext.JobGraph.InsertOne(new SetDto
            {
                Id = ObjectId.GenerateNewId(),
                Key = "some-set<4>",
                Value = "4",
                Score = 0.0,
                SetType = "some-set"
            }.Serialize());
            _dbContext.JobGraph.InsertOne(new SetDto
            {
                Id = ObjectId.GenerateNewId(),
                Key = "some-set<5>",
                Value = "5",
                Score = 0.0,
                SetType = "some-set"
            }.Serialize());
            _dbContext.JobGraph.InsertOne(new SetDto
            {
                Id = ObjectId.GenerateNewId(),
                Key = "some-set<6>",
                Value = "6",
                Score = 0.0,
                SetType = "some-set"
            }.Serialize());
            // Act
            var result = _connection.GetAllItemsFromSet("some-set");

            // Assert
            Assert.Equal(5, result.Count);
            Assert.Contains("1", result);
            Assert.Contains("2", result);
            Assert.Equal(new[] { "1", "2", "4", "5", "6" }, result);
        }

        [Fact]
        public void GetAllItemsFromSet_ReturnsAllItems_WithCorrectValues()
        {
            // Arrange
            using (var t = _connection.CreateWriteTransaction())
            {
                t.AddToSet("some-set", "11:22");
                t.AddToSet("some-set", "33");
                t.Commit();
            }

            // Act
            var result = _connection.GetAllItemsFromSet("some-set");

            // Assert
            Assert.Equal(new[] { "11:22", "33" }, result);
        }

        [Fact]
        public void GetAllItemsFromSet_ReturnsAllItems_WhenKeyContainsRegexSpecialChars()
        {
            var key = "some+-[regex]?-#set";
            // Arrange
            using (var t = _connection.CreateWriteTransaction())
            {
                t.AddToSet(key, "11:22");
                t.AddToSet(key, "33");
                t.Commit();
            }

            // Act
            var result = _connection.GetAllItemsFromSet(key);

            // Assert
            Assert.Equal(new[] { "11:22", "33" }, result);
        }

        [Fact]
        public void SetRangeInHash_ThrowsAnException_WhenKeyIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => _connection.SetRangeInHash(null, new Dictionary<string, string>()));

            Assert.Equal("key", exception.ParamName);
        }

        [Fact]
        public void SetRangeInHash_ThrowsAnException_WhenKeyValuePairsArgumentIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => _connection.SetRangeInHash("some-hash", null));

            Assert.Equal("keyValuePairs", exception.ParamName);
        }

        [Fact]
        public void SetRangeInHash_MergesAllRecords()
        {
            _connection.SetRangeInHash("some-hash", new Dictionary<string, string>
            {
                { "Key1", "Value1" },
                { "Key2", "Value2" }
            });
            var filter = new BsonDocument
            {
                [nameof(HashDto.Key)] = "some-hash",
                ["_t"] = nameof(HashDto)
            };
            var result = new HashDto(_dbContext.JobGraph.Find(filter).First()).Fields;

            Assert.Equal("Value1", result["Key1"]);
            Assert.Equal("Value2", result["Key2"]);
        }

        [Fact]
        public void GetAllEntriesFromHash_ThrowsAnException_WhenKeyIsNull()
        {
            Assert.Throws<ArgumentNullException>(() => _connection.GetAllEntriesFromHash(null));
        }

        [Fact]
        public void GetAllEntriesFromHash_ReturnsNull_IfHashDoesNotExist()
        {
            var result = _connection.GetAllEntriesFromHash("some-hash");
            Assert.Null(result);
        }

        [Fact]
        public void GetAllEntriesFromHash_ReturnsAllKeysAndTheirValues()
        {
            // Arrange
            var someHash = new HashDto
            {
                Id = ObjectId.GenerateNewId(),
                Key = "some-hash",
                Fields = new Dictionary<string, string>
                {
                    ["Key1"] = "Value1",
                    ["Key2"] = "Value2",
                },
            }.Serialize();
            _dbContext.JobGraph.InsertOne(someHash);
            var anotherHash = new HashDto
            {
                Id = ObjectId.GenerateNewId(),
                Key = "another-hash",
                Fields = new Dictionary<string, string>
                {
                    ["Key3"] = "Value3"
                },
            }.Serialize();
            _dbContext.JobGraph.InsertOne(anotherHash);

            // Act
            var result = _connection.GetAllEntriesFromHash("some-hash");

            // Assert
            Assert.NotNull(result);
            Assert.Equal(2, result.Count);
            Assert.Equal("Value1", result["Key1"]);
            Assert.Equal("Value2", result["Key2"]);
        }

        [Fact]
        public void GetSetCount_ThrowsAnException_WhenKeyIsNull()
        {
            Assert.Throws<ArgumentNullException>(() => _connection.GetSetCount(null));
        }

        [Fact]
        public void GetSetCount_ReturnsZero_WhenSetDoesNotExist()
        {
            var result = _connection.GetSetCount("my-set");
            Assert.Equal(0, result);
        }

        [Fact]
        public void GetSetCount_ReturnsNumberOfElements_InASet()
        {
            _dbContext.JobGraph.InsertOne(new SetDto
            {
                Id = ObjectId.GenerateNewId(),
                Key = "set-1<value-1>",
                Value = "value-1",
                SetType = "set-1"
            }.Serialize());
            _dbContext.JobGraph.InsertOne(new SetDto
            {
                Id = ObjectId.GenerateNewId(),
                Key = "set-2<value-1>",
                Value = "value-1",
                SetType = "set-2"
            }.Serialize());
            _dbContext.JobGraph.InsertOne(new SetDto
            {
                Id = ObjectId.GenerateNewId(),
                Key = "set-1<value-2>",
                Value = "value-2",
                SetType = "set-1"
            }.Serialize());

            var result = _connection.GetSetCount("set-1");

            Assert.Equal(2, result);
        }

        [Fact]
        public void GetSetCount_ReturnsNumberOfElements_InASet_WhenKeyContainsRegexSpecialChars()
        {
            var key = "some+-[regex]?-#set";

            _dbContext.JobGraph.InsertOne(new SetDto
            {
                Id = ObjectId.GenerateNewId(),
                Key = $"{key}<value-1>",
                Value = "value-1",
                SetType = key
            }.Serialize());
            _dbContext.JobGraph.InsertOne(new SetDto
            {
                Id = ObjectId.GenerateNewId(),
                Key = $"{key}<value-2>",
                Value = "value-2",
                SetType = key
            }.Serialize());

            var result = _connection.GetSetCount(key);

            Assert.Equal(2, result);
        }

        [Fact]
        public void GetRangeFromSet_ThrowsAnException_WhenKeyIsNull()
        {
            Assert.Throws<ArgumentNullException>(() => _connection.GetRangeFromSet(null, 0, 1));
        }

        [Fact]
        public void GetRangeFromSet_ReturnsPagedElementsInCorrectOrder()
        {
            _dbContext.JobGraph.InsertOne(new SetDto
            {
                Id = ObjectId.GenerateNewId(),
                Key = "set-1<1>",
                Value = "1",
                Score = 0.0,
                SetType = "set-1"
            }.Serialize());

            _dbContext.JobGraph.InsertOne(new SetDto
            {
                Id = ObjectId.GenerateNewId(),
                Key = "set-1<2>",
                Value = "2",
                Score = 0.0,
                SetType = "set-1"
            }.Serialize());

            _dbContext.JobGraph.InsertOne(new SetDto
            {
                Id = ObjectId.GenerateNewId(),
                Key = "set-1<3>",
                Value = "3",
                Score = 0.0,
                SetType = "set-1"
            }.Serialize());

            _dbContext.JobGraph.InsertOne(new SetDto
            {
                Id = ObjectId.GenerateNewId(),
                Key = "set-1<4>",
                Value = "4",
                Score = 0.0,
                SetType = "set-1"
            }.Serialize());

            _dbContext.JobGraph.InsertOne(new SetDto
            {
                Id = ObjectId.GenerateNewId(),
                Key = "set-2<5>",
                Value = "5",
                Score = 0.0,
                SetType = "set-2"
            }.Serialize());

            _dbContext.JobGraph.InsertOne(new SetDto
            {
                Id = ObjectId.GenerateNewId(),
                Key = "set-1<6>",
                Value = "6",
                Score = 0.0,
                SetType = "set-1"
            }.Serialize());

            var result = _connection.GetRangeFromSet("set-1", 1, 8);

            Assert.Equal(new[] { "2", "3", "4", "6" }, result);
        }

        [Fact]
        public void GetRangeFromSet_ReturnsPagedElementsInCorrectOrder_WhenKeyContainsRegexSpecialChars()
        {
            var key = "some+-[regex]?-#set";

            _dbContext.JobGraph.InsertOne(new SetDto
            {
                Id = ObjectId.GenerateNewId(),
                Key = $"{key}<1>",
                Value = "1",
                Score = 0.0,
                SetType = key
            }.Serialize());

            _dbContext.JobGraph.InsertOne(new SetDto
            {
                Id = ObjectId.GenerateNewId(),
                Key = $"{key}<2>",
                Value = "2",
                Score = 0.0,
                SetType = key
            }.Serialize());

            _dbContext.JobGraph.InsertOne(new SetDto
            {
                Id = ObjectId.GenerateNewId(),
                Key = $"{key}<3>",
                Value = "3",
                Score = 0.0,
                SetType = key
            }.Serialize());

            _dbContext.JobGraph.InsertOne(new SetDto
            {
                Id = ObjectId.GenerateNewId(),
                Key = $"{key}<4>",
                Value = "4",
                Score = 0.0,
                SetType = key
            }.Serialize());

            _dbContext.JobGraph.InsertOne(new SetDto
            {
                Id = ObjectId.GenerateNewId(),
                Key = $"{key}<6>",
                Value = "6",
                Score = 0.0,
                SetType = key
            }.Serialize());

            var result = _connection.GetRangeFromSet(key, 1, 8);

            Assert.Equal(new[] { "2", "3", "4", "6" }, result);
        }

        [Fact]
        public void GetSetTtl_ThrowsAnException_WhenKeyIsNull()
        {
            Assert.Throws<ArgumentNullException>(() => _connection.GetSetTtl(null));
        }

        [Fact]
        public void GetSetTtl_ReturnsNegativeValue_WhenSetDoesNotExist()
        {
            var result = _connection.GetSetTtl("my-set");
            Assert.True(result < TimeSpan.Zero, $"{result} < {TimeSpan.Zero}");
        }

        [Fact]
        public void GetSetTtl_ReturnsExpirationTime_OfAGivenSet()
        {
            // Arrange
            _dbContext.JobGraph.InsertOne(new SetDto
            {
                Id = ObjectId.GenerateNewId(),
                Key = "set-1<1>",
                Score = 0.0,
                ExpireAt = DateTime.UtcNow.AddMinutes(60),
                SetType = "set-1"
            }.Serialize());

            _dbContext.JobGraph.InsertOne(new SetDto
            {
                Id = ObjectId.GenerateNewId(),
                Key = "set-2<2>",
                Score = 0.0,
                ExpireAt = null,
                SetType = "set-2"
            }.Serialize());

            // Act
            var result = _connection.GetSetTtl("set-1");

            // Assert
            Assert.True(TimeSpan.FromMinutes(59) < result);
            Assert.True(result < TimeSpan.FromMinutes(61));
        }

        [Fact]
        public void GetSetTtl_ReturnsExpirationTime_OfAGivenSet_WhenKeyContainsRegexSpecialChars()
        {
            var key = "some+-[regex]?-#set";

            // Arrange
            _dbContext.JobGraph.InsertOne(new SetDto
            {
                Id = ObjectId.GenerateNewId(),
                Key = $"{key}<1>",
                Score = 0.0,
                ExpireAt = DateTime.UtcNow.AddMinutes(60),
                SetType = key
            }.Serialize());

            // Act
            var result = _connection.GetSetTtl(key);

            // Assert
            Assert.True(TimeSpan.FromMinutes(59) < result);
            Assert.True(result < TimeSpan.FromMinutes(61));
        }

        [Fact]
        public void GetCounter_ThrowsAnException_WhenKeyIsNull()
        {
            Assert.Throws<ArgumentNullException>(() => _connection.GetCounter(null));
        }

        [Fact]
        public void GetCounter_ReturnsZero_WhenKeyDoesNotExist()
        {
            var result = _connection.GetCounter("my-counter");
            Assert.Equal(0, result);
        }

        [Fact]
        public void GetCounter_ReturnsSumOfValues_InCounterTable()
        {
            // Arrange
            _dbContext.JobGraph.InsertOne(new CounterDto
            {
                Id = ObjectId.GenerateNewId(),
                Key = "counter-1",
                Value = 2L
            }.Serialize());
            _dbContext.JobGraph.InsertOne(new CounterDto
            {
                Id = ObjectId.GenerateNewId(),
                Key = "counter-2",
                Value = 1L
            }.Serialize());

            // Act
            var result = _connection.GetCounter("counter-1");

            // Assert
            Assert.Equal(2, result);
        }

        [Fact]
        public void GetHashCount_ThrowsAnException_WhenKeyIsNull()
        {
            Assert.Throws<ArgumentNullException>(() => _connection.GetHashCount(null));
        }

        [Fact]
        public void GetHashCount_ReturnsZero_WhenKeyDoesNotExist()
        {
            var result = _connection.GetHashCount("my-hash");
            Assert.Equal(0, result);
        }

        [Fact]
        public void GetHashCount_ReturnsNumber_OfHashFields()
        {
            // Arrange
            _dbContext.JobGraph.InsertOne(new HashDto
            {
                Id = ObjectId.GenerateNewId(),
                Fields = new Dictionary<string, string>
                {
                    ["field-1"] = "field-1-value",
                    ["field-2"] = "field-2-value",

                },
                Key = "hash-1",
            }.Serialize());
            _dbContext.JobGraph.InsertOne(new HashDto
            {
                Id = ObjectId.GenerateNewId(),
                Key = "hash-2",
                Fields = new Dictionary<string, string>
                {
                    ["field-1"] = "field-1-value",

                },
            }.Serialize());

            // Act
            var result = _connection.GetHashCount("hash-1");

            // Assert
            Assert.Equal(2, result);
        }

        [Fact]
        public void GetHashTtl_ThrowsAnException_WhenKeyIsNull()
        {
            Assert.Throws<ArgumentNullException>(() => _connection.GetHashTtl(null));
        }

        [Fact]
        public void GetHashTtl_ReturnsNegativeValue_WhenHashDoesNotExist()
        {
            var result = _connection.GetHashTtl("my-hash");
            Assert.True(result < TimeSpan.Zero);
        }

        [Fact]
        public void GetHashTtl_ReturnsExpirationTimeForHash()
        {
            // Arrange
            _dbContext.JobGraph.InsertOne(new HashDto
            {
                Id = ObjectId.GenerateNewId(),
                Key = "hash-1",
                Fields = new Dictionary<string, string>
                {
                    ["field-1"] = "field-1-value",

                },
                ExpireAt = DateTime.UtcNow.AddHours(1)
            }.Serialize());
            _dbContext.JobGraph.InsertOne(new HashDto
            {
                Id = ObjectId.GenerateNewId(),
                Key = "hash-2",
                Fields = new Dictionary<string, string>
                {
                    ["field-1"] = "field-1-value",

                },
                ExpireAt = null
            }.Serialize());

            // Act
            var result = _connection.GetHashTtl("hash-1");

            // Assert
            Assert.True(TimeSpan.FromMinutes(59) < result);
            Assert.True(result < TimeSpan.FromMinutes(61));
        }

        [Fact]
        public void GetValueFromHash_ThrowsAnException_WhenKeyIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(() => _connection.GetValueFromHash(null, "name"));

            Assert.Equal("key", exception.ParamName);
        }

        [Fact]
        public void GetValueFromHash_ThrowsAnException_WhenNameIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(() => _connection.GetValueFromHash("key", null));

            Assert.Equal("name", exception.ParamName);
        }

        [Fact]
        public void GetValueFromHash_ReturnsNull_WhenHashDoesNotExist()
        {
            var result = _connection.GetValueFromHash("my-hash", "name");
            Assert.Null(result);
        }

        [Fact]
        public void GetValueFromHash_ReturnsValue_OfAGivenField()
        {
            // Arrange
            _dbContext.JobGraph.InsertOne(new HashDto
            {
                Id = ObjectId.GenerateNewId(),
                Key = "hash-1",
                Fields = new Dictionary<string, string>
                {
                    ["field-1"] = "1",
                    ["field-2"] = "2",
                },
            }.Serialize());
            _dbContext.JobGraph.InsertOne(new HashDto
            {
                Id = ObjectId.GenerateNewId(),
                Key = "hash-2",
                Fields = new Dictionary<string, string>
                {
                    ["field-1"] = "2"
                },
            }.Serialize());

            // Act
            var result = _connection.GetValueFromHash("hash-1", "field-1");

            // Assert
            Assert.Equal("1", result);
        }

        [Fact]
        public void GetListCount_ThrowsAnException_WhenKeyIsNull()
        {
            Assert.Throws<ArgumentNullException>(() => _connection.GetListCount(null));
        }

        [Fact]
        public void GetListCount_ReturnsZero_WhenListDoesNotExist()
        {
            var result = _connection.GetListCount("my-list");
            Assert.Equal(0, result);
        }

        [Fact]
        public void GetListCount_ReturnsTheNumberOfListElements()
        {
            // Arrange
            _dbContext.JobGraph.InsertOne(new ListDto
            {
                Id = ObjectId.GenerateNewId(),
                Item = "list-1",
            }.Serialize());
            _dbContext.JobGraph.InsertOne(new ListDto
            {
                Id = ObjectId.GenerateNewId(),
                Item = "list-1",
            }.Serialize());
            _dbContext.JobGraph.InsertOne(new ListDto
            {
                Id = ObjectId.GenerateNewId(),
                Item = "list-2",
            }.Serialize());

            // Act
            var result = _connection.GetListCount("list-1");

            // Assert
            Assert.Equal(2, result);
        }

        [Fact]
        public void GetListTtl_ThrowsAnException_WhenKeyIsNull()
        {
            Assert.Throws<ArgumentNullException>(() => _connection.GetListTtl(null));
        }

        [Fact]
        public void GetListTtl_ReturnsNegativeValue_WhenListDoesNotExist()
        {
            var result = _connection.GetListTtl("my-list");
            Assert.True(result < TimeSpan.Zero);
        }

        [Fact]
        public void GetListTtl_ReturnsExpirationTimeForList()
        {
            // Arrange
            _dbContext.JobGraph.InsertOne(new ListDto
            {
                Id = ObjectId.GenerateNewId(),
                Item = "list-1",
                ExpireAt = DateTime.UtcNow.AddHours(1)
            }.Serialize());
            _dbContext.JobGraph.InsertOne(new ListDto
            {
                Id = ObjectId.GenerateNewId(),
                Item = "list-2",
                ExpireAt = null
            }.Serialize());

            // Act
            var result = _connection.GetListTtl("list-1");

            // Assert
            Assert.True(TimeSpan.FromMinutes(59) < result);
            Assert.True(result < TimeSpan.FromMinutes(61));
        }

        [Fact]
        public void GetRangeFromList_ThrowsAnException_WhenKeyIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(() => _connection.GetRangeFromList(null, 0, 1));

            Assert.Equal("key", exception.ParamName);
        }

        [Fact]
        public void GetRangeFromList_ReturnsAnEmptyList_WhenListDoesNotExist()
        {
            var result = _connection.GetRangeFromList("my-list", 0, 1);
            Assert.Empty(result);
        }

        [Fact]
        public void GetRangeFromList_ReturnsAllEntries_WithinGivenBounds()
        {
            // Arrange
            _dbContext.JobGraph.InsertOne(new ListDto
            {
                Id = ObjectId.GenerateNewId(),
                Item = "list-1",
                Value = "1"
            }.Serialize());
            _dbContext.JobGraph.InsertOne(new ListDto
            {
                Id = ObjectId.GenerateNewId(),
                Item = "list-2",
                Value = "2"
            }.Serialize());
            _dbContext.JobGraph.InsertOne(new ListDto
            {
                Id = ObjectId.GenerateNewId(),
                Item = "list-1",
                Value = "3"
            }.Serialize());
            _dbContext.JobGraph.InsertOne(new ListDto
            {
                Id = ObjectId.GenerateNewId(),
                Item = "list-1",
                Value = "4"
            }.Serialize());
            _dbContext.JobGraph.InsertOne(new ListDto
            {
                Id = ObjectId.GenerateNewId(),
                Item = "list-1",
                Value = "5"
            }.Serialize());

            // Act
            var result = _connection.GetRangeFromList("list-1", 1, 2);

            // Assert
            Assert.Equal(new[] { "4", "3" }, result);
        }

        [Fact]
        public void GetRangeFromList_ReturnsAllEntriesInCorrectOrder()
        {
            // Arrange
            var listDtos = new List<ListDto>
            {
                new ListDto
                {
                    Id = ObjectId.GenerateNewId(),
                    Item = "list-1",
                    Value = "1"
                },
                new ListDto
                {
                    Id = ObjectId.GenerateNewId(),
                    Item = "list-1",
                    Value = "2"
                },
                new ListDto
                {
                    Id = ObjectId.GenerateNewId(),
                    Item = "list-1",
                    Value = "3"
                },
                new ListDto
                {
                    Id = ObjectId.GenerateNewId(),
                    Item = "list-1",
                    Value = "4"
                },
                new ListDto
                {
                    Id = ObjectId.GenerateNewId(),
                    Item = "list-1",
                    Value = "5"
                }
            };
            _dbContext.JobGraph.InsertMany(listDtos.Select(l => l.Serialize()));

            // Act
            var result = _connection.GetRangeFromList("list-1", 1, 5);

            // Assert
            Assert.Equal(new[] { "4", "3", "2", "1" }, result);
        }

        [Fact]
        public void GetAllItemsFromList_ThrowsAnException_WhenKeyIsNull()
        {
            Assert.Throws<ArgumentNullException>(() => _connection.GetAllItemsFromList(null));
        }

        [Fact]
        public void GetAllItemsFromList_ReturnsAnEmptyList_WhenListDoesNotExist()
        {
            var result = _connection.GetAllItemsFromList("my-list");
            Assert.Empty(result);
        }

        [Fact]
        public void GetAllItemsFromList_ReturnsAllItemsFromAGivenList_InCorrectOrder()
        {
            // Arrange
            _dbContext.JobGraph.InsertOne(new ListDto
            {
                Id = ObjectId.GenerateNewId(),
                Item = "list-1",
                Value = "1"
            }.Serialize());
            _dbContext.JobGraph.InsertOne(new ListDto
            {
                Id = ObjectId.GenerateNewId(),
                Item = "list-2",
                Value = "2"
            }.Serialize());
            _dbContext.JobGraph.InsertOne(new ListDto
            {
                Id = ObjectId.GenerateNewId(),
                Item = "list-1",
                Value = "3"
            }.Serialize());
            _dbContext.JobGraph.InsertOne(new ListDto
            {
                Id = ObjectId.GenerateNewId(),
                Item = "list-1",
                Value = "4"
            }.Serialize());
            _dbContext.JobGraph.InsertOne(new ListDto
            {
                Id = ObjectId.GenerateNewId(),
                Item = "list-1",
                Value = "5"
            }.Serialize());

            // Act
            var result = _connection.GetAllItemsFromList("list-1");

            // Assert
            Assert.Equal(new[] { "5", "4", "3", "1" }, result);
        }

        [Fact]
        public void GetUtcDateTime_FromConnection_Success()
        {
            // Arrange
            _dbContext.Schema.InsertOne(new SchemaDto{Version = MongoSchema.None}.Serialize());
            
            // Act
            var serverTime = _connection.GetUtcDateTime();

            // Assert
            Assert.Equal(DateTime.UtcNow.Date, serverTime.Date);
        }

        [Fact]
        public void GetSetCount_LimitedWhenSetsArgumentIsEmpty_ReturnsZero()
        {
            // Arrange

            // Act
            var result = _connection.GetSetCount(Enumerable.Empty<string>(), 10);

            // Assert
            Assert.Equal(0, result);
        }

        [Fact]
        public void GetSetCount_LimitedWhenGivenSetsDoNotExist_ReturnsZero()
        {
            // Arrange

            // Act
            var result = _connection.GetSetCount(new[] { "set-1", "set-2" }, 10);

            // Assert
            Assert.Equal(0, result);
        }

        [Fact]
        public void GetSetCount_LimitedOfGivenSetCardinalities_ReturnsTheSum()
        {
            // Arrange
            _dbContext.JobGraph.InsertMany(new List<BsonDocument>
            {
                new SetDto{SetType = "set-1", Value = "1"}.Serialize(),
                new SetDto{SetType = "set-1", Value = "2"}.Serialize(),
                new SetDto{SetType = "set-2", Value = "2"}.Serialize(),
                new SetDto{SetType = "set-2", Value = "3"}.Serialize(),
                new SetDto{SetType = "set-3", Value = "1"}.Serialize(),
            });

            // Act
            var result = _connection.GetSetCount(new[] { "set-1", "set-2" }, 10);
            

            // Assert
            Assert.Equal(4, result);
        }

        [Fact]
        public void GetSetCount_LimitedIsConsidered_LimitValue()
        {
            // Arrange
            _dbContext.JobGraph.InsertMany(new List<BsonDocument>
            {
                new SetDto{SetType = "set-1", Value = "1"}.Serialize(),
                new SetDto{SetType = "set-1", Value = "2"}.Serialize(),
                new SetDto{SetType = "set-2", Value = "2"}.Serialize(),
                new SetDto{SetType = "set-2", Value = "3"}.Serialize(),
                new SetDto{SetType = "set-3", Value = "1"}.Serialize(),
            });

            // Act
            var result = _connection.GetSetCount(new[] { "set-1", "set-2" }, 2);


            // Assert
            Assert.Equal(2, result);
        }

    }

#pragma warning restore 1591
}