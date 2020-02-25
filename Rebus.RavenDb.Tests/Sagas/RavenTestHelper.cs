using System;
using Raven.Client.Documents;
using Raven.Embedded;

namespace Rebus.RavenDb.Tests.Sagas
{
    static class RavenTestHelper
    {
        static RavenTestHelper()
        {
            try
            {
                EmbeddedServer.Instance.StartServer();
            }
            catch (Exception exception)
            {
                throw new ApplicationException("Exception when starting embedded RavenDB server", exception);
            }

            AppDomain.CurrentDomain.DomainUnload += (o, ea) =>
            {
                try
                {
                    EmbeddedServer.Instance.Dispose();
                }
                catch (Exception exception)
                {
                    Console.WriteLine($"Exception when stopping embedded RavenDB server: {exception}");
                }
            };
        }

        static int _counter = 1;
        
        public static IDocumentStore GetDocumentStore()
        {
            var databaseName = $"rebustestdb_{_counter++}";
            Console.WriteLine($"Getting document store named '{databaseName}'");
            var documentStore = EmbeddedServer.Instance.GetDocumentStore(databaseName);
            documentStore.Initialize();
            return documentStore;
        }
    }
}