using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Reflection;
using System.Diagnostics;
using DeNSo;


#if NETFX_CORE
using System.Composition;
using System.Composition.Hosting;
#else
using System.ComponentModel.Composition;
using System.ComponentModel.Composition.Hosting;
#endif

namespace DeNSo
{
  /// <summary>
  /// DensoExtensions use MEF to load extensions in densodb
  /// </summary>
  public class DensoExtensions
  {
#if WINDOWS
    private AggregateCatalog catalog = new AggregateCatalog();
#endif

    [ImportMany(typeof(IExtensionPlugin))]
    public IExtensionPlugin[] Extensions { get; set; }

    [ImportMany(typeof(ICommandHandler))]
    public ICommandHandler[] ImportedHandlers { get; set; }

#if WINDOWS
    public DensoExtensions()
    {

      catalog.Catalogs.Add(new AssemblyCatalog(Assembly.GetExecutingAssembly()));
    }

    public void RegisterExtensionAssembly(Assembly assembly)
    {
      catalog.Catalogs.Add(new AssemblyCatalog(assembly));
    }
#endif

    public void Init()
    {

#if WINDOWS
      AddDirectoryCatalog(catalog, "Extensions");
      AddDirectoryCatalog(catalog, "EventHandlers");
#endif

#if NETFX_CORE
      var configuration = new ContainerConfiguration().WithAssembly(this.GetType().GetTypeInfo().Assembly);
      CompositionHost host = configuration.CreateContainer();
      host.SatisfyImports(this);
#else
      CompositionContainer container = new CompositionContainer(catalog);
      container.ComposeParts(this);
#endif

      if (Extensions != null)
        foreach (var plugin in Extensions)
        {
          plugin.Init();
        }

      EventHandlerManager.AnalyzeCommandHandlers(ImportedHandlers);
    }

#if WINDOWS
    private static void AddDirectoryCatalog(AggregateCatalog catalog, string directoryname)
    {
      try
      {
        var c1 = new DirectoryCatalog(directoryname);
        c1.Refresh();
        catalog.Catalogs.Add(c1);
      }
      catch (Exception ex)
      {
        Debug.WriteLine(ex.Message);
        //LogWriter.LogMessage("Error occurred while composing Denso Extensions", LogEntryType.Error);
        //LogWriter.LogException(ex);
      }
    }
#endif

  }
}
