using System;
using System.Collections.Generic;
using Hangfire.Mongo.Database;
using Hangfire.Mongo.Dto;
using Hangfire.Mongo.Tests.Utils;
using MongoDB.Bson;
using MongoDB.Driver;
using Xunit;

namespace Hangfire.Mongo.Tests
{
#pragma warning disable 1591
	[Collection("Database")]
	public class StateDtoDataFieldSerializerFacts
	{
		[Fact, CleanDatabase]
		public void StateDtoDataField_IsBsonTypeDocument_Deserialized()
		{
			UseConnection(database =>
			{
				// ARRANGE
				var state = new StateDto
				{
					CreatedAt = DateTime.Now,
					Data = new Dictionary<string, string> { ["Name"] = "Value"},
					Id = ObjectId.GenerateNewId(),
					JobId = 1,
					Name = "Name",
					Reason = "Reason"
				};

				database.State.InsertOne(state);

				// ACT
				var retrievedState = database
					.State
					.Find(Builders<StateDto>.Filter.Eq(_ => _.JobId, state.JobId))
					.FirstOrDefault();

				// ASSERT
				Assert.NotNull(retrievedState);
				Assert.NotNull(retrievedState.Data);
				Assert.Equal(state.Data, retrievedState.Data);
			});
		}

		[Fact, CleanDatabase]
		public void StateDtoDataField_IsBsonTypeString_Deserialized()
		{
			UseConnection(database =>
			{
				// ARRANGE
				var state = new StateDto
				{
					CreatedAt = DateTime.Now,
					Data = new Dictionary<string, string> { ["Name"] = "Value" },
					Id = ObjectId.GenerateNewId(),
					JobId = 1,
					Name = "Name",
					Reason = "Reason"
				};
				var bsonState = state.ToBsonDocument();
				bsonState[nameof(StateDto.Data)] = "{ 'Name': 'Value' }";
				database
					.Database
					.GetCollection<BsonDocument>(database.State.CollectionNamespace.CollectionName)
					.InsertOne(bsonState);

				// ACT
				var retrievedState = database
					.State
					.Find(Builders<StateDto>.Filter.Eq(_ => _.JobId, state.JobId))
					.FirstOrDefault();

				// ASSERT
				Assert.NotNull(retrievedState);
				Assert.NotNull(retrievedState.Data);
				Assert.Equal(state.Data, retrievedState.Data);
			});
		}

		private static void UseConnection(Action<HangfireDbContext> action)
		{
			using (var connection = ConnectionUtils.CreateConnection())
			{
				action(connection);
			}
		}
	}
#pragma warning restore 1591
}
