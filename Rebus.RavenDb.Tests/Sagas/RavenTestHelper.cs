using System;
using System.IO;
using Raven.Client.Documents;
using Raven.Embedded;

namespace Rebus.RavenDb.Tests.Sagas;

static class RavenTestHelper
{
    static readonly Lazy<EmbeddedServer> ServerInstance = new(() =>
    {
        var instance = EmbeddedServer.Instance;
        var temporaryDirectory = Path.Combine(AppContext.BaseDirectory, Guid.NewGuid().ToString("N"));

        instance.StartServer(new ServerOptions { DataDirectory = temporaryDirectory });

        AppDomain.CurrentDomain.DomainUnload += (_, _) =>
        {
            instance.Dispose();

            try
            {
                Directory.Delete(temporaryDirectory, recursive: true);
            }
            catch (Exception exception)
            {
                Console.WriteLine($"ERROR when deleting temp directory '{temporaryDirectory}': {exception}");
            }
        };

        return instance;
    });

    static int _counter = 1;

    public static IDocumentStore GetDocumentStore()
    {
        var databaseName = $"rebustestdb_{_counter++}";
        Console.WriteLine($"Getting document store named '{databaseName}'");
        var documentStore = ServerInstance.Value.GetDocumentStore(databaseName);
        documentStore.Initialize();
        return documentStore;
    }
}