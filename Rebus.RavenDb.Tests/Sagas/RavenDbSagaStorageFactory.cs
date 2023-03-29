using Raven.Client.Documents;
using Rebus.RavenDb.Sagas;
using Rebus.Sagas;
using Rebus.Tests.Contracts.Sagas;

namespace Rebus.RavenDb.Tests.Sagas;

public class RavenDbSagaStorageFactory : ISagaStorageFactory
{
    public IDocumentStore DocumentStore { get; } = RavenTestHelper.GetDocumentStore();

    public ISagaStorage GetSagaStorage() => new RavenDbSagaStorage(DocumentStore);

    public void CleanUp() => DocumentStore.Dispose();
}