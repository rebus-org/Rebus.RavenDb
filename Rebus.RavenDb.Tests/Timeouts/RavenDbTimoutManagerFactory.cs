using System;
using Raven.Client.Documents;
using Rebus.Logging;
using Rebus.RavenDb.Tests.Sagas;
using Rebus.RavenDb.Timouts;
using Rebus.Tests.Contracts.Timeouts;
using Rebus.Time;
using Rebus.Timeouts;

namespace Rebus.RavenDb.Tests.Timeouts
{
    public class RavenDbTimoutManagerFactory : ITimeoutManagerFactory
    {
        readonly FakeRebusTime _fakeRebusTime = new FakeRebusTime();
        readonly IDocumentStore _documentStore = RavenTestHelper.GetDocumentStore();

        public RavenDbTimoutManagerFactory()
        {
            _documentStore.ExecuteIndex(new TimeoutIndex());
        }

        public void FakeIt(DateTimeOffset fakeTime)
        {
            _fakeRebusTime.SetNow(fakeTime);
        }

        public ITimeoutManager Create() => new RavenDbTimeoutManager(_documentStore, new ConsoleLoggerFactory(false), _fakeRebusTime);

        public void Cleanup() => _documentStore.Dispose();

        public string GetDebugInfo()
        {
            return "could not provide debug info for this particular timeout manager.... implement if needed :)";
        }

        class FakeRebusTime : IRebusTime
        {
            Func<DateTimeOffset> _nowFactory = () => DateTimeOffset.Now;

            public DateTimeOffset Now => _nowFactory();

            public void SetNow(DateTimeOffset fakeTime) => _nowFactory = () => fakeTime;
        }
    }
}