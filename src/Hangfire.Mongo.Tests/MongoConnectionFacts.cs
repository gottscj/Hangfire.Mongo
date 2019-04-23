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
using Moq;
using Xunit;

namespace Hangfire.Mongo.Tests
{
#pragma warning disable 1591
    [Collection("Database")]
    public class MongoConnectionFacts
    {
        private readonly HangfireDbContext _dbContext;
        private readonly MongoConnection _connection;
		private readonly Mock<IJobQueueSemaphore> _jobQueueSemaphoreMock;

        public MongoConnectionFacts()
        {
		    _jobQueueSemaphoreMock = new Mock<IJobQueueSemaphore>(MockBehavior.Strict);
            _dbContext = ConnectionUtils.CreateDbContext();
            _connection = new MongoConnection(_dbContext, new MongoStorageOptions(), _jobQueueSemaphoreMock.Object);
        }
        
        [Fact]
        public void Ctor_ThrowsAnException_WhenConnectionIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new MongoConnection(null, null, _jobQueueSemaphoreMock.Object));

            Assert.Equal("database", exception.ParamName);
        }

        [Fact, CleanDatabase]
        public void Ctor_ThrowsAnException_WhenProvidersCollectionIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new MongoConnection(_dbContext, null, _jobQueueSemaphoreMock.Object));

            Assert.Equal("storageOptions", exception.ParamName);
        }

        [Fact, CleanDatabase]
        public void FetchNextJob_DelegatesItsExecution_ToTheQueue()
        {
            var token = new CancellationToken();
            var queues = new[] { "default" };
            var jobQueueDto = new JobQueueDto
            {
                Id = ObjectId.GenerateNewId(),
                Queue = "default",
                FetchedAt = null,
                JobId = ObjectId.GenerateNewId()
            };
            _jobQueueSemaphoreMock.Setup(m => m.WaitNonBlock("default"));
                
            _dbContext.JobGraph.InsertOne(jobQueueDto);
                
            var fetchedJob = _connection.FetchNextJob(queues, token);

            Assert.Equal(fetchedJob.JobId, jobQueueDto.JobId.ToString());
            
            _jobQueueSemaphoreMock.Verify(m => m.WaitNonBlock("default"), Times.Once);
        }

        [Fact, CleanDatabase]
        public void CreateWriteTransaction_ReturnsNonNullInstance()
        {
            var transaction = _connection.CreateWriteTransaction();
            Assert.NotNull(transaction);
        }

        [Fact, CleanDatabase]
        public void AcquireLock_ReturnsNonNullInstance()
        {
            var @lock = _connection.AcquireDistributedLock("1", TimeSpan.FromSeconds(1));
            Assert.NotNull(@lock);
        }

        [Fact, CleanDatabase]
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

        [Fact, CleanDatabase]
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

        [Fact, CleanDatabase]
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

            var databaseJob = _dbContext.JobGraph.OfType<JobDto>().Find(new BsonDocument()).ToList().Single();
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
                .JobGraph.OfType<JobDto>()
                .Find(Builders<JobDto>.Filter.Eq(_ => _.Id, ObjectId.Parse(jobId)))
                .Project(j => j.Parameters)
                .ToList()
                .SelectMany(j => j)
                .ToDictionary(p => p.Key, x => x.Value);

            Assert.NotNull(parameters);
            Assert.Equal("Value1", parameters["Key1"]);
            Assert.Equal("Value2", parameters["Key2"]);
        }

        [Fact, CleanDatabase]
        public void GetJobData_ThrowsAnException_WhenJobIdIsNull()
        {
            Assert.Throws<ArgumentNullException>(() => _connection.GetJobData(null));
        }

        [Fact, CleanDatabase]
        public void GetJobData_ReturnsNull_WhenThereIsNoSuchJob()
        {
            var result = _connection.GetJobData(ObjectId.GenerateNewId().ToString());
            Assert.Null(result);
        }

        [Fact, CleanDatabase]
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
            _dbContext.JobGraph.InsertOne(jobDto);

            var result = _connection.GetJobData(jobDto.Id.ToString());

            Assert.NotNull(result);
            Assert.NotNull(result.Job);
            Assert.Equal(SucceededState.StateName, result.State);
            Assert.Equal("Arguments", result.Job.Args[0]);
            Assert.Null(result.LoadException);
            Assert.True(DateTime.UtcNow.AddMinutes(-1) < result.CreatedAt);
            Assert.True(result.CreatedAt < DateTime.UtcNow.AddMinutes(1));
        }

        [Fact, CleanDatabase]
        public void GetStateData_ThrowsAnException_WhenJobIdIsNull()
        {
            Assert.Throws<ArgumentNullException>(() => _connection.GetStateData(null));
        }

        [Fact, CleanDatabase]
        public void GetStateData_ReturnsNull_IfThereIsNoSuchState()
        {
            var result = _connection.GetStateData(ObjectId.GenerateNewId().ToString());
            Assert.Null(result);
        }

        [Fact, CleanDatabase]
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

            _dbContext.JobGraph.InsertOne(jobDto);
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

            _dbContext.JobGraph.OfType<JobDto>().UpdateOne(j => j.Id == jobId, update);

            var result = _connection.GetStateData(jobId.ToString());
            Assert.NotNull(result);

            Assert.Equal("Name", result.Name);
            Assert.Equal("Reason", result.Reason);
            Assert.Equal("Value", result.Data["Key"]);
        }

        [Fact, CleanDatabase]
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
            _dbContext.JobGraph.InsertOne(jobDto);
            var jobId = jobDto.Id;

            var result = _connection.GetJobData(jobId.ToString());

            Assert.NotNull(result.LoadException);
        }

        [Fact, CleanDatabase]
        public void SetParameter_ThrowsAnException_WhenJobIdIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(() => _connection.SetJobParameter(null, "name", "value"));

            Assert.Equal("id", exception.ParamName);
        }

        [Fact, CleanDatabase]
        public void SetParameter_ThrowsAnException_WhenNameIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => _connection.SetJobParameter("547527b4c6b6cc26a02d021d", null, "value"));

            Assert.Equal("name", exception.ParamName);
        }

        [Fact, CleanDatabase]
        public void SetParameters_CreatesNewParameter_WhenParameterWithTheGivenNameDoesNotExists()
        {
            var jobDto = new JobDto
            {
                Id = ObjectId.GenerateNewId(),
                InvocationData = "",
                Arguments = "",
                CreatedAt = DateTime.UtcNow
            };
            _dbContext.JobGraph.InsertOne(jobDto);
            var jobId = jobDto.Id;

            _connection.SetJobParameter(jobId.ToString(), "Name", "Value");

            var parameters = _dbContext
                .JobGraph.OfType<JobDto>()
                .Find(j => j.Id == jobId)
                .Project(j => j.Parameters)
                .FirstOrDefault();

            Assert.NotNull(parameters);
            Assert.Equal("Value", parameters["Name"]);
        }

        [Fact, CleanDatabase]
        public void SetParameter_UpdatesValue_WhenParameterWithTheGivenName_AlreadyExists()
        {
            var jobDto = new JobDto
            {
                Id = ObjectId.GenerateNewId(),
                InvocationData = "",
                Arguments = "",
                CreatedAt = DateTime.UtcNow
            };
            _dbContext.JobGraph.InsertOne(jobDto);
            var jobId = jobDto.Id;

            _connection.SetJobParameter(jobId.ToString(), "Name", "Value");
            _connection.SetJobParameter(jobId.ToString(), "Name", "AnotherValue");

            var parameters = _dbContext
                .JobGraph.OfType<JobDto>()
                .Find(j => j.Id == jobId)
                .Project(j => j.Parameters)
                .FirstOrDefault();

            Assert.NotNull(parameters);
            Assert.Equal("AnotherValue", parameters["Name"]);
        }

        [Fact, CleanDatabase]
        public void SetParameter_CanAcceptNulls_AsValues()
        {
            var jobDto = new JobDto
            {
                Id = ObjectId.GenerateNewId(),
                InvocationData = "",
                Arguments = "",
                CreatedAt = DateTime.UtcNow
            };
            _dbContext.JobGraph.InsertOne(jobDto);
            var jobId = jobDto.Id;

            _connection.SetJobParameter(jobId.ToString(), "Name", null);

            var parameters = _dbContext
                .JobGraph.OfType<JobDto>()
                .Find(j => j.Id == jobId)
                .Project(j => j.Parameters)
                .FirstOrDefault();

            Assert.NotNull(parameters);
            Assert.Null(parameters["Name"]);
        }

        [Fact, CleanDatabase]
        public void GetParameter_ThrowsAnException_WhenJobIdIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => _connection.GetJobParameter(null, "hello"));

            Assert.Equal("id", exception.ParamName);
        }

        [Fact, CleanDatabase]
        public void GetParameter_ThrowsAnException_WhenNameIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => _connection.GetJobParameter("547527b4c6b6cc26a02d021d", null));

            Assert.Equal("name", exception.ParamName);
        }

        [Fact, CleanDatabase]
        public void GetParameter_ReturnsNull_WhenParameterDoesNotExists()
        {
            var value = _connection.GetJobParameter(ObjectId.GenerateNewId().ToString(), "hello");
            Assert.Null(value);
        }

        [Fact, CleanDatabase]
        public void GetParameter_ReturnsParameterValue_WhenJobExists()
        {
            var jobDto = new JobDto
            {
                Id = ObjectId.GenerateNewId(),
                InvocationData = "",
                Arguments = "",
                CreatedAt = DateTime.UtcNow
            };
            _dbContext.JobGraph.InsertOne(jobDto);


            _connection.SetJobParameter(jobDto.Id.ToString(), "name", "value");

            var value = _connection.GetJobParameter(jobDto.Id.ToString(), "name");

            Assert.Equal("value", value);
        }

        [Fact, CleanDatabase]
        public void GetFirstByLowestScoreFromSet_ThrowsAnException_WhenKeyIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => _connection.GetFirstByLowestScoreFromSet(null, 0, 1));

            Assert.Equal("key", exception.ParamName);
        }

        [Fact, CleanDatabase]
        public void GetFirstByLowestScoreFromSet_ThrowsAnException_ToScoreIsLowerThanFromScore()
        {
            Assert.Throws<ArgumentException>(
                () => _connection.GetFirstByLowestScoreFromSet("key", 0, -1));
        }

        [Fact, CleanDatabase]
        public void GetFirstByLowestScoreFromSet_ReturnsNull_WhenTheKeyDoesNotExist()
        {
            var result = _connection.GetFirstByLowestScoreFromSet(
                "key", 0, 1);

            Assert.Null(result);
        }

        [Fact, CleanDatabase]
        public void GetFirstByLowestScoreFromSet_ReturnsTheValueWithTheLowestScore()
        {
            _dbContext.JobGraph.InsertOne(new SetDto
            {
                Id = ObjectId.GenerateNewId(),
                Key = "key<1.0>",
                Value = "1.0",
                Score = 1.0
            });
            _dbContext.JobGraph.InsertOne(new SetDto
            {
                Id = ObjectId.GenerateNewId(),
                Key = "key<-1.0>",
                Value = "-1.0",
                Score = -1.0,
            });
            _dbContext.JobGraph.InsertOne(new SetDto
            {
                Id = ObjectId.GenerateNewId(),
                Key = "key<-5.0>",
                Value = "-5.0",
                Score = -5.0
            });
            _dbContext.JobGraph.InsertOne(new SetDto
            {
                Id = ObjectId.GenerateNewId(),
                Key = "another-key<-2.0>",
                Value = "-2.0",
                Score = -2.0
            });

            var result = _connection.GetFirstByLowestScoreFromSet("key", -1.0, 3.0);

            Assert.Equal("-1.0", result);
        }

        [Fact, CleanDatabase]
        public void AnnounceServer_ThrowsAnException_WhenServerIdIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => _connection.AnnounceServer(null, new ServerContext()));

            Assert.Equal("serverId", exception.ParamName);
        }

        [Fact, CleanDatabase]
        public void AnnounceServer_ThrowsAnException_WhenContextIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => _connection.AnnounceServer("server", null));

            Assert.Equal("context", exception.ParamName);
        }

        [Fact, CleanDatabase]
        public void AnnounceServer_CreatesOrUpdatesARecord()
        {
            var context1 = new ServerContext
            {
                Queues = new[] { "critical", "default" },
                WorkerCount = 4
            };
            _connection.AnnounceServer("server", context1);

            var server = _dbContext.Server.Find(new BsonDocument()).Single();
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
            var sameServer = _dbContext.Server.Find(new BsonDocument()).Single();
            Assert.Equal("server", sameServer.Id);
            Assert.Equal(context2.WorkerCount, sameServer.WorkerCount);
        }

        [Fact, CleanDatabase]
        public void RemoveServer_ThrowsAnException_WhenServerIdIsNull()
        {
            Assert.Throws<ArgumentNullException>(() => _connection.RemoveServer(null));
        }

        [Fact, CleanDatabase]
        public void RemoveServer_RemovesAServerRecord()
        {
            _dbContext.Server.InsertOne(new ServerDto
            {
                Id = "Server1",
                LastHeartbeat = DateTime.UtcNow
            });
            _dbContext.Server.InsertOne(new ServerDto
            {
                Id = "Server2",
                LastHeartbeat = DateTime.UtcNow
            });

            _connection.RemoveServer("Server1");

            var server = _dbContext.Server.Find(new BsonDocument()).ToList().Single();
            Assert.NotEqual("Server1", server.Id, StringComparer.OrdinalIgnoreCase);
        }

        [Fact, CleanDatabase]
        public void Heartbeat_ThrowsAnException_WhenServerIdIsNull()
        {
            Assert.Throws<ArgumentNullException>(
                () => _connection.Heartbeat(null));
        }

        [Fact, CleanDatabase]
        public void Heartbeat_UpdatesLastHeartbeat_OfTheServerWithGivenId()
        {
            _dbContext.Server.InsertOne(new ServerDto
            {
                Id = "server1",
                LastHeartbeat = new DateTime(2012, 12, 12, 12, 12, 12, DateTimeKind.Utc)
            });
            _dbContext.Server.InsertOne(new ServerDto
            {
                Id = "server2",
                LastHeartbeat = new DateTime(2012, 12, 12, 12, 12, 12, DateTimeKind.Utc)
            });

            _connection.Heartbeat("server1");

            var servers = _dbContext.Server.Find(new BsonDocument()).ToList()
                .ToDictionary(x => x.Id, x => x.LastHeartbeat);

            Assert.True(servers.ContainsKey("server1"));
            Assert.True(servers.ContainsKey("server2"));
            Assert.NotEqual(2012, servers["server1"].Value.Year);
            Assert.Equal(2012, servers["server2"].Value.Year);
        }

        [Fact, CleanDatabase]
        public void RemoveTimedOutServers_ThrowsAnException_WhenTimeOutIsNegative()
        {
            Assert.Throws<ArgumentException>(
                () => _connection.RemoveTimedOutServers(TimeSpan.FromMinutes(-5)));
        }

        [Fact, CleanDatabase]
        public void RemoveTimedOutServers_DoItsWorkPerfectly()
        {
            var id1 = ObjectId.GenerateNewId();
            var id2 = ObjectId.GenerateNewId();

            var id2Newest = id2 > id1;
            _dbContext.Server.InsertOne(new ServerDto
            {
                Id = "server1",
                LastHeartbeat = DateTime.UtcNow.AddDays(-1)
            });
            _dbContext.Server.InsertOne(new ServerDto
            {
                Id = "server2",
                LastHeartbeat = DateTime.UtcNow.AddHours(-12)
            });

            _connection.RemoveTimedOutServers(TimeSpan.FromHours(15));

            var liveServer = _dbContext.Server.Find(new BsonDocument()).ToList().Single();
            Assert.Equal("server2", liveServer.Id);
        }

        [Fact, CleanDatabase]
        public void GetAllItemsFromSet_ThrowsAnException_WhenKeyIsNull()
        {
            Assert.Throws<ArgumentNullException>(() => _connection.GetAllItemsFromSet(null));
        }

        [Fact, CleanDatabase]
        public void GetAllItemsFromSet_ReturnsEmptyCollection_WhenKeyDoesNotExist()
        {
            var result = _connection.GetAllItemsFromSet("some-set");

            Assert.NotNull(result);
            Assert.Empty(result);
        }

        [Fact, CleanDatabase]
        public void GetAllItemsFromSet_ReturnsAllItems_InCorrectOrder()
        {
            // Arrange
            _dbContext.JobGraph.InsertOne(new SetDto
            {
                Id = ObjectId.GenerateNewId(),
                Key = "some-set<1>",
                Value = "1",
                Score = 0.0
            });
            _dbContext.JobGraph.InsertOne(new SetDto
            {
                Id = ObjectId.GenerateNewId(),
                Key = "some-set<2>",
                Value = "2",
                Score = 0.0
            });
            _dbContext.JobGraph.InsertOne(new SetDto
            {
                Id = ObjectId.GenerateNewId(),
                Key = "another-set<3>",
                Value = "3",
                Score = 0.0
            });
            _dbContext.JobGraph.InsertOne(new SetDto
            {
                Id = ObjectId.GenerateNewId(),
                Key = "some-set<4>",
                Value = "4",
                Score = 0.0
            });
            _dbContext.JobGraph.InsertOne(new SetDto
            {
                Id = ObjectId.GenerateNewId(),
                Key = "some-set<5>",
                Value = "5",
                Score = 0.0
            });
            _dbContext.JobGraph.InsertOne(new SetDto
            {
                Id = ObjectId.GenerateNewId(),
                Key = "some-set<6>",
                Value = "6",
                Score = 0.0
            });
            // Act
            var result = _connection.GetAllItemsFromSet("some-set");

            // Assert
            Assert.Equal(5, result.Count);
            Assert.Contains("1", result);
            Assert.Contains("2", result);
            Assert.Equal(new[] { "1", "2", "4", "5", "6" }, result);
        }
        
        [Fact, CleanDatabase]
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

        [Fact, CleanDatabase]
        public void SetRangeInHash_ThrowsAnException_WhenKeyIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => _connection.SetRangeInHash(null, new Dictionary<string, string>()));

            Assert.Equal("key", exception.ParamName);
        }

        [Fact, CleanDatabase]
        public void SetRangeInHash_ThrowsAnException_WhenKeyValuePairsArgumentIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => _connection.SetRangeInHash("some-hash", null));

            Assert.Equal("keyValuePairs", exception.ParamName);
        }

        [Fact, CleanDatabase]
        public void SetRangeInHash_MergesAllRecords()
        {
            _connection.SetRangeInHash("some-hash", new Dictionary<string, string>
            {
                { "Key1", "Value1" },
                { "Key2", "Value2" }
            });

            var result = _dbContext.JobGraph.OfType<HashDto>()
                .Find(Builders<HashDto>.Filter.Eq(_ => _.Key, "some-hash"))
                .First()
                .Fields;

            Assert.Equal("Value1", result["Key1"]);
            Assert.Equal("Value2", result["Key2"]);
        }

        [Fact, CleanDatabase]
        public void GetAllEntriesFromHash_ThrowsAnException_WhenKeyIsNull()
        {
            Assert.Throws<ArgumentNullException>(() => _connection.GetAllEntriesFromHash(null));
        }

        [Fact, CleanDatabase]
        public void GetAllEntriesFromHash_ReturnsNull_IfHashDoesNotExist()
        {
            var result = _connection.GetAllEntriesFromHash("some-hash");
            Assert.Null(result);
        }

        [Fact, CleanDatabase]
        public void GetAllEntriesFromHash_ReturnsAllKeysAndTheirValues()
        {
            // Arrange
            _dbContext.JobGraph.InsertOne(new HashDto
            {
                Id = ObjectId.GenerateNewId(),
                Key = "some-hash",
                Fields = new Dictionary<string, string>
                {
                    ["Key1"] = "Value1",
                    ["Key2"] = "Value2",
                },
            });
            _dbContext.JobGraph.InsertOne(new HashDto
            {
                Id = ObjectId.GenerateNewId(),
                Key = "another-hash",
                Fields = new Dictionary<string, string>
                {
                    ["Key3"] = "Value3"
                },
            });

            // Act
            var result = _connection.GetAllEntriesFromHash("some-hash");

            // Assert
            Assert.NotNull(result);
            Assert.Equal(2, result.Count);
            Assert.Equal("Value1", result["Key1"]);
            Assert.Equal("Value2", result["Key2"]);
        }

        [Fact, CleanDatabase]
        public void GetSetCount_ThrowsAnException_WhenKeyIsNull()
        {
            Assert.Throws<ArgumentNullException>(() => _connection.GetSetCount(null));
        }

        [Fact, CleanDatabase]
        public void GetSetCount_ReturnsZero_WhenSetDoesNotExist()
        {
            var result = _connection.GetSetCount("my-set");
            Assert.Equal(0, result);
        }

        [Fact, CleanDatabase]
        public void GetSetCount_ReturnsNumberOfElements_InASet()
        {
            _dbContext.JobGraph.InsertOne(new SetDto
            {
                Id = ObjectId.GenerateNewId(),
                Key = "set-1<value-1>",
                Value = "value-1"
            });
            _dbContext.JobGraph.InsertOne(new SetDto
            {
                Id = ObjectId.GenerateNewId(),
                Key = "set-2<value-1>",
                Value = "value-1"
            });
            _dbContext.JobGraph.InsertOne(new SetDto
            {
                Id = ObjectId.GenerateNewId(),
                Key = "set-1<value-2>",
                Value = "value-2"
            });

            var result = _connection.GetSetCount("set-1");

            Assert.Equal(2, result);
        }

        [Fact, CleanDatabase]
        public void GetRangeFromSet_ThrowsAnException_WhenKeyIsNull()
        {
            Assert.Throws<ArgumentNullException>(() => _connection.GetRangeFromSet(null, 0, 1));
        }

        [Fact, CleanDatabase]
        public void GetRangeFromSet_ReturnsPagedElementsInCorrectOrder()
        {
            _dbContext.JobGraph.InsertOne(new SetDto
            {
                Id = ObjectId.GenerateNewId(),
                Key = "set-1<1>",
                Value = "1",
                Score = 0.0
            });

            _dbContext.JobGraph.InsertOne(new SetDto
            {
                Id = ObjectId.GenerateNewId(),
                Key = "set-1<2>",
                Value = "2",
                Score = 0.0
            });

            _dbContext.JobGraph.InsertOne(new SetDto
            {
                Id = ObjectId.GenerateNewId(),
                Key = "set-1<3>",
                Value = "3",
                Score = 0.0
            });

            _dbContext.JobGraph.InsertOne(new SetDto
            {
                Id = ObjectId.GenerateNewId(),
                Key = "set-1<4>",
                Value = "4",
                Score = 0.0
            });

            _dbContext.JobGraph.InsertOne(new SetDto
            {
                Id = ObjectId.GenerateNewId(),
                Key = "set-2<5>",
                Value = "5",
                Score = 0.0
            });

            _dbContext.JobGraph.InsertOne(new SetDto
            {
                Id = ObjectId.GenerateNewId(),
                Key = "set-1<6>",
                Value = "6",
                Score = 0.0
            });

            var result = _connection.GetRangeFromSet("set-1", 1, 8);

            Assert.Equal(new[] { "2", "3", "4", "6" }, result);
        }

        [Fact, CleanDatabase]
        public void GetSetTtl_ThrowsAnException_WhenKeyIsNull()
        {
            Assert.Throws<ArgumentNullException>(() => _connection.GetSetTtl(null));
        }

        [Fact, CleanDatabase]
        public void GetSetTtl_ReturnsNegativeValue_WhenSetDoesNotExist()
        {
            var result = _connection.GetSetTtl("my-set");
            Assert.True(result < TimeSpan.Zero);
        }

        [Fact, CleanDatabase]
        public void GetSetTtl_ReturnsExpirationTime_OfAGivenSet()
        {
            // Arrange
            _dbContext.JobGraph.InsertOne(new SetDto
            {
                Id = ObjectId.GenerateNewId(),
                Key = "set-1<1>",
                Score = 0.0,
                ExpireAt = DateTime.UtcNow.AddMinutes(60)
            });

            _dbContext.JobGraph.InsertOne(new SetDto
            {
                Id = ObjectId.GenerateNewId(),
                Key = "set-2<2>",
                Score = 0.0,
                ExpireAt = null
            });

            // Act
            var result = _connection.GetSetTtl("set-1");

            // Assert
            Assert.True(TimeSpan.FromMinutes(59) < result);
            Assert.True(result < TimeSpan.FromMinutes(61));
        }

        [Fact, CleanDatabase]
        public void GetCounter_ThrowsAnException_WhenKeyIsNull()
        {
            Assert.Throws<ArgumentNullException>(() => _connection.GetCounter(null));
        }

        [Fact, CleanDatabase]
        public void GetCounter_ReturnsZero_WhenKeyDoesNotExist()
        {
            var result = _connection.GetCounter("my-counter");
            Assert.Equal(0, result);
        }

        [Fact, CleanDatabase]
        public void GetCounter_ReturnsSumOfValues_InCounterTable()
        {
            // Arrange
            _dbContext.JobGraph.InsertOne(new CounterDto
            {
                Id = ObjectId.GenerateNewId(),
                Key = "counter-1",
                Value = 2L
            });
            _dbContext.JobGraph.InsertOne(new CounterDto
            {
                Id = ObjectId.GenerateNewId(),
                Key = "counter-2",
                Value = 1L
            });
                
            // Act
            var result = _connection.GetCounter("counter-1");

            // Assert
            Assert.Equal(2, result);
        }

        [Fact, CleanDatabase]
        public void GetHashCount_ThrowsAnException_WhenKeyIsNull()
        {
            Assert.Throws<ArgumentNullException>(() => _connection.GetHashCount(null));
        }

        [Fact, CleanDatabase]
        public void GetHashCount_ReturnsZero_WhenKeyDoesNotExist()
        {
            var result = _connection.GetHashCount("my-hash");
            Assert.Equal(0, result);
        }

        [Fact, CleanDatabase]
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
            });
            _dbContext.JobGraph.InsertOne(new HashDto
            {
                Id = ObjectId.GenerateNewId(),
                Key = "hash-2",
                Fields = new Dictionary<string, string>
                {
                    ["field-1"] = "field-1-value",
                        
                },
            });

            // Act
            var result = _connection.GetHashCount("hash-1");

            // Assert
            Assert.Equal(2, result);
        }

        [Fact, CleanDatabase]
        public void GetHashTtl_ThrowsAnException_WhenKeyIsNull()
        {
            Assert.Throws<ArgumentNullException>(() => _connection.GetHashTtl(null));
        }

        [Fact, CleanDatabase]
        public void GetHashTtl_ReturnsNegativeValue_WhenHashDoesNotExist()
        {
            var result = _connection.GetHashTtl("my-hash");
            Assert.True(result < TimeSpan.Zero);
        }

        [Fact, CleanDatabase]
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
            });
            _dbContext.JobGraph.InsertOne(new HashDto
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
            var result = _connection.GetHashTtl("hash-1");

            // Assert
            Assert.True(TimeSpan.FromMinutes(59) < result);
            Assert.True(result < TimeSpan.FromMinutes(61));
        }

        [Fact, CleanDatabase]
        public void GetValueFromHash_ThrowsAnException_WhenKeyIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(() => _connection.GetValueFromHash(null, "name"));

            Assert.Equal("key", exception.ParamName);
        }

        [Fact, CleanDatabase]
        public void GetValueFromHash_ThrowsAnException_WhenNameIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(() => _connection.GetValueFromHash("key", null));

            Assert.Equal("name", exception.ParamName);
        }

        [Fact, CleanDatabase]
        public void GetValueFromHash_ReturnsNull_WhenHashDoesNotExist()
        {
            var result = _connection.GetValueFromHash("my-hash", "name");
            Assert.Null(result);
        }

        [Fact, CleanDatabase]
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
            });
            _dbContext.JobGraph.InsertOne(new HashDto
            {
                Id = ObjectId.GenerateNewId(),
                Key = "hash-2",
                Fields = new Dictionary<string, string>
                {
                    ["field-1"] = "2"
                },
            });

            // Act
            var result = _connection.GetValueFromHash("hash-1", "field-1");

            // Assert
            Assert.Equal("1", result);
        }

        [Fact, CleanDatabase]
        public void GetListCount_ThrowsAnException_WhenKeyIsNull()
        {
            Assert.Throws<ArgumentNullException>(() => _connection.GetListCount(null));
        }

        [Fact, CleanDatabase]
        public void GetListCount_ReturnsZero_WhenListDoesNotExist()
        {
            var result = _connection.GetListCount("my-list");
            Assert.Equal(0, result);
        }

        [Fact, CleanDatabase]
        public void GetListCount_ReturnsTheNumberOfListElements()
        {
            // Arrange
            _dbContext.JobGraph.InsertOne(new ListDto
            {
                Id = ObjectId.GenerateNewId(),
                Item = "list-1",
            });
            _dbContext.JobGraph.InsertOne(new ListDto
            {
                Id = ObjectId.GenerateNewId(),
                Item = "list-1",
            });
            _dbContext.JobGraph.InsertOne(new ListDto
            {
                Id = ObjectId.GenerateNewId(),
                Item = "list-2",
            });

            // Act
            var result = _connection.GetListCount("list-1");

            // Assert
            Assert.Equal(2, result);
        }

        [Fact, CleanDatabase]
        public void GetListTtl_ThrowsAnException_WhenKeyIsNull()
        {
            Assert.Throws<ArgumentNullException>(() => _connection.GetListTtl(null));
        }

        [Fact, CleanDatabase]
        public void GetListTtl_ReturnsNegativeValue_WhenListDoesNotExist()
        {
            var result = _connection.GetListTtl("my-list");
            Assert.True(result < TimeSpan.Zero);
        }

        [Fact, CleanDatabase]
        public void GetListTtl_ReturnsExpirationTimeForList()
        {
            // Arrange
            _dbContext.JobGraph.InsertOne(new ListDto
            {
                Id = ObjectId.GenerateNewId(),
                Item = "list-1",
                ExpireAt = DateTime.UtcNow.AddHours(1)
            });
            _dbContext.JobGraph.InsertOne(new ListDto
            {
                Id = ObjectId.GenerateNewId(),
                Item = "list-2",
                ExpireAt = null
            });

            // Act
            var result = _connection.GetListTtl("list-1");

            // Assert
            Assert.True(TimeSpan.FromMinutes(59) < result);
            Assert.True(result < TimeSpan.FromMinutes(61));
        }

        [Fact, CleanDatabase]
        public void GetRangeFromList_ThrowsAnException_WhenKeyIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(() => _connection.GetRangeFromList(null, 0, 1));

            Assert.Equal("key", exception.ParamName);
        }

        [Fact, CleanDatabase]
        public void GetRangeFromList_ReturnsAnEmptyList_WhenListDoesNotExist()
        {
            var result = _connection.GetRangeFromList("my-list", 0, 1);
            Assert.Empty(result);
        }

        [Fact, CleanDatabase]
        public void GetRangeFromList_ReturnsAllEntries_WithinGivenBounds()
        {
            // Arrange
            _dbContext.JobGraph.InsertOne(new ListDto
            {
                Id = ObjectId.GenerateNewId(),
                Item = "list-1",
                Value = "1"
            });
            _dbContext.JobGraph.InsertOne(new ListDto
            {
                Id = ObjectId.GenerateNewId(),
                Item = "list-2",
                Value = "2"
            });
            _dbContext.JobGraph.InsertOne(new ListDto
            {
                Id = ObjectId.GenerateNewId(),
                Item = "list-1",
                Value = "3"
            });
            _dbContext.JobGraph.InsertOne(new ListDto
            {
                Id = ObjectId.GenerateNewId(),
                Item = "list-1",
                Value = "4"
            });
            _dbContext.JobGraph.InsertOne(new ListDto
            {
                Id = ObjectId.GenerateNewId(),
                Item = "list-1",
                Value = "5"
            });

            // Act
            var result = _connection.GetRangeFromList("list-1", 1, 2);

            // Assert
            Assert.Equal(new[] { "4", "3" }, result);
        }

        [Fact, CleanDatabase]
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
            _dbContext.JobGraph.InsertMany(listDtos);

            // Act
            var result = _connection.GetRangeFromList("list-1", 1, 5);

            // Assert
            Assert.Equal(new[] { "4", "3", "2", "1" }, result);
        }

        [Fact, CleanDatabase]
        public void GetAllItemsFromList_ThrowsAnException_WhenKeyIsNull()
        {
            Assert.Throws<ArgumentNullException>(() => _connection.GetAllItemsFromList(null));
        }

        [Fact, CleanDatabase]
        public void GetAllItemsFromList_ReturnsAnEmptyList_WhenListDoesNotExist()
        {
            var result = _connection.GetAllItemsFromList("my-list");
            Assert.Empty(result);
        }

        [Fact, CleanDatabase]
        public void GetAllItemsFromList_ReturnsAllItemsFromAGivenList_InCorrectOrder()
        {
            // Arrange
            _dbContext.JobGraph.InsertOne(new ListDto
            {
                Id = ObjectId.GenerateNewId(),
                Item = "list-1",
                Value = "1"
            });
            _dbContext.JobGraph.InsertOne(new ListDto
            {
                Id = ObjectId.GenerateNewId(),
                Item = "list-2",
                Value = "2"
            });
            _dbContext.JobGraph.InsertOne(new ListDto
            {
                Id = ObjectId.GenerateNewId(),
                Item = "list-1",
                Value = "3"
            });
            _dbContext.JobGraph.InsertOne(new ListDto
            {
                Id = ObjectId.GenerateNewId(),
                Item = "list-1",
                Value = "4"
            });
            _dbContext.JobGraph.InsertOne(new ListDto
            {
                Id = ObjectId.GenerateNewId(),
                Item = "list-1",
                Value = "5"
            });

            // Act
            var result = _connection.GetAllItemsFromList("list-1");

            // Assert
            Assert.Equal(new[] { "5", "4", "3", "1" }, result);
        }

    }

#pragma warning restore 1591
}