using System;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;
using MongoDB.Driver.Builders;

namespace Hangfire.Mongo.MongoUtils
{
	public class AutoIncrementIdGenerator : IIdGenerator
	{
		private readonly string _prefix;

		public AutoIncrementIdGenerator()
			: this(String.Empty)
		{
			
		}
		public AutoIncrementIdGenerator(string prefix)
		{
			_prefix = prefix ?? String.Empty;
		}

		public object GenerateId(object container, object document)
		{
			var idSequenceCollection = ((MongoCollection) container).Database
				.GetCollection(_prefix + "_identifiers");

			var query = Query.EQ("_id", ((MongoCollection)container).Name);

			return (idSequenceCollection.FindAndModify(new FindAndModifyArgs
			{
				Query = query,
				Update = Update.Inc("seq", 1),
				VersionReturned = FindAndModifyDocumentVersion.Modified,
				Upsert = true
			}).ModifiedDocument["seq"]).AsInt32;
		}

		public bool IsEmpty(object id)
		{
			return (int)id == 0;
		}
	}
}