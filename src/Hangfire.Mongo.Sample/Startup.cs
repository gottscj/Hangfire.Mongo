using Hangfire.Mongo.Sample;
using Microsoft.Owin;
using Owin;

[assembly: OwinStartup(typeof(Startup))]
namespace Hangfire.Mongo.Sample
{
    public partial class Startup
    {
        public void Configuration(IAppBuilder app)
        {
            ConfigureHangfire(app);
        }
    }
}