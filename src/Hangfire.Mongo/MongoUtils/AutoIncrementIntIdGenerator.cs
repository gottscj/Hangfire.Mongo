namespace Hangfire.Mongo.MongoUtils
{
    /// <summary>
    /// Represents ID generator for Mongo database (integer)
    /// </summary>
    public class AutoIncrementIntIdGenerator : AutoIncrementIdGenerator
    {
        /// <summary>
        /// Converts sequence number into appropriate format
        /// </summary>
        /// <param name="input">Number</param>
        /// <returns>Converted number</returns>
        protected override object FormatNumber(long input)
        {
            return (int) input;
        }
    }
}