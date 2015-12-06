using System;
using Microsoft.AspNet.Identity;
using Microsoft.AspNet.Identity.Owin;
using Microsoft.Owin;
using Microsoft.Owin.Security.Cookies;
using Microsoft.Owin.Security.Google;
using Owin;
using Hangfire.Mongo.Sample.Models;

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