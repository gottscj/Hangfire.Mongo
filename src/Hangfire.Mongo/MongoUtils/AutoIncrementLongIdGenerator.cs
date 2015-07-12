namespace Hangfire.Mongo.MongoUtils
{
    /// <summary>
    /// Represents ID generator for Mongo database (long)
    /// </summary>
    public class AutoIncrementLongIdGenerator : AutoIncrementIdGenerator
    {
        /// <summary>
        /// Converts sequence number into appropriate format
        /// </summary>
        /// <param name="input">Number</param>
        /// <returns>Converted number</returns>
        protected override object FormatNumber(long input)
        {
            return input;
        }
    }
}