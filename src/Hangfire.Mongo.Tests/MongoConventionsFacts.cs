using Hangfire.Mongo.Tests.Utils;
using MongoDB.Bson.Serialization.Conventions;
using Xunit;

namespace Hangfire.Mongo.Tests
{

    [Collection("Database")]
    public class MongoConventionsFacts
    {

        [Fact, CleanDatabase(false)]
        public void Conventions_UsesOwnConventionsForDtoNameSpace_WhenCamelCaseIsRegistered()
        {
            // ARRANGE
            var conventionPack = new ConventionPack { new CamelCaseElementNameConvention() };
            ConventionRegistry.Register("camelCase", conventionPack, t => true);

            // ACT
            // This line will throw during migration if camelCase is active for the Dto namespace.
            var connection = ConnectionUtils.CreateConnection();

            // ASSERT
            Assert.NotNull(connection);
        }

    }

}
