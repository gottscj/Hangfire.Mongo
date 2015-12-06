using Owin;

namespace Hangfire.Mongo.Sample
{
    public partial class Startup
    {
        public void ConfigureHangfire(IAppBuilder app)
        {
            GlobalConfiguration.Configuration.UseMongoStorage("mongodb://localhost", "hangfire-mongo-sample");

            app.UseHangfireServer();
            app.UseHangfireDashboard();
        }
    }
}
