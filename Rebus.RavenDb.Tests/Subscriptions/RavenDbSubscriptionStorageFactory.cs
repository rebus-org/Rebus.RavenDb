using System;
using System.Collections.Concurrent;
using Rebus.Logging;
using Rebus.RavenDb.Subscriptions;
using Rebus.RavenDb.Tests.Sagas;
using Rebus.Subscriptions;
using Rebus.Tests.Contracts.Subscriptions;

namespace Rebus.RavenDb.Tests.Subscriptions;

public class RavenDbSubscriptionStorageFactory : ISubscriptionStorageFactory
{
    readonly ConcurrentStack<IDisposable> _disposables = new ConcurrentStack<IDisposable>();

    public ISubscriptionStorage Create()
    {
        var documentStore = RavenTestHelper.GetDocumentStore();

        _disposables.Push(documentStore);

        return new RavenDbSubscriptionStorage(documentStore, true, new ConsoleLoggerFactory(false));
    }

    public void Cleanup()
    {
        while (_disposables.TryPop(out var disposable))
        {
            disposable.Dispose();
        }
    }
}