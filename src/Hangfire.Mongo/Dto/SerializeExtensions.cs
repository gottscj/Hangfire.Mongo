using MongoDB.Bson;
using System;
using System.Collections.Generic;
using System.Text;

namespace Hangfire.Mongo.Dto
{
    internal static class SerializeExtensions
    {
        public static string StringOrNull(this BsonValue bsonValue)
        {
            if (bsonValue is BsonNull)
            {
                return null;
            }

            return bsonValue.AsString;
        }

        public static BsonValue ToBsonValue(this string value)
        {
            if(value == null)
            {
                return BsonNull.Value;
            }
            return (BsonString)value;
        }
    }
}
