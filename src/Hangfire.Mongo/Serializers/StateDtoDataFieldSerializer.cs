using System.Collections.Generic;
using System.Linq;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Serializers;

namespace Hangfire.Mongo.Serializers
{
	/// <summary>
	/// This serializer is introduced to support legacy StateDto.Date representation as json string in mongo db.
	/// This will most likely be removed again, when a migration strategy has been introduced.
	/// </summary>
	public class StateDtoDataFieldSerializer : SerializerBase<Dictionary<string, string>>
	{
		/// <summary>
		/// Serialize using default serializer
		/// </summary>
		public override void Serialize(BsonSerializationContext context, BsonSerializationArgs args, Dictionary<string, string> value)
		{
			BsonSerializer.Serialize(context.Writer, typeof(Dictionary<string, string>), value);
		}

		/// <summary>
		/// Deserialize field represented as document or json object to Dictionary
		/// </summary>
		public override Dictionary<string, string> Deserialize(BsonDeserializationContext context,
			BsonDeserializationArgs args)
		{
			if (context.Reader.CurrentBsonType == BsonType.String)
			{
				var json = context.Reader.ReadString();
				if (string.IsNullOrEmpty(json))
				{
					return new Dictionary<string, string>();
				}
				var document = BsonDocument.Parse(json);
				return document.ToDictionary(element => element.Name, element => element.Value.AsString);
			}

			return (Dictionary<string, string>)BsonSerializer.Deserialize(context.Reader, typeof(Dictionary<string, string>));
		}
	}
}