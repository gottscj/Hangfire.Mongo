using Xunit;

namespace Hangfire.Mongo.Tests.Utils
{
    [CollectionDefinition("Database")]
    public class DatabaseCollection : ICollectionFixture<MongoIntegrationTestFixture>
    {
    }
}
