using Microsoft.Web.Infrastructure.DynamicModuleHelper;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

[assembly: PreApplicationStartMethod(typeof(DeNSo.Server.HttpModules.AuthenticationModule), "Initialize")]
namespace DeNSo.Server.HttpModules
{
  public class AuthenticationModule : IHttpModule
  {
    public static void Initialize()
    {
      DynamicModuleUtility.RegisterModule(typeof(AuthenticationModule));
    }

    public void Dispose()
    {
    }

    public void Init(HttpApplication context)
    {
      context.PostAuthenticateRequest += context_PostAuthenticateRequest;
    }

    void context_PostAuthenticateRequest(object sender, EventArgs e)
    {
      HttpApplication app = sender as HttpApplication;
      if (app != null)
      {
        //var db = DeNSo.Session.New;
        //var result = db.Get<Joshua.Models.SecurityUser>((Joshua.Models.SecurityUser u) => u.UserName == app.User.Identity.Name).FirstOrDefault();
        //if (result != null)
        //{
        //  app.Context.User = new Joshua.Security.JPrincipal(new Security.JIdentity(result));
        //}
        // app.User.Identity.Name
      }
    }
  }
}
