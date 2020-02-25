using Rebus.Extensions;
using Rebus.Logging;
using Rebus.Messages;
using Rebus.Timeouts;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Commands.Batches;
using Rebus.Time;

#pragma warning disable 1998

namespace Rebus.RavenDb.Timouts
{
    /// <summary>
    /// Implementation of <see cref="ITimeoutManager"/> that stores timeouts in RavenDB
    /// </summary>
    public class RavenDbTimeoutManager : ITimeoutManager
    {
        readonly IDocumentStore _documentStore;
        readonly IRebusTime _rebusTime;
        readonly ILog _log;

        /// <summary>
        /// Creates the timeout manager, using the given document store to store <see cref="Timeout"/> documents
        /// </summary>
        public RavenDbTimeoutManager(IDocumentStore documentStore, IRebusLoggerFactory rebusLoggerFactory, IRebusTime rebusTime)
        {
            if (rebusLoggerFactory == null) throw new ArgumentNullException(nameof(rebusLoggerFactory));
            _documentStore = documentStore ?? throw new ArgumentNullException(nameof(documentStore));
            _rebusTime = rebusTime ?? throw new ArgumentNullException(nameof(rebusTime));
            _log = rebusLoggerFactory.GetLogger<RavenDbTimeoutManager>();
        }

        /// <inheritdoc />
        public async Task Defer(DateTimeOffset approximateDueTime, Dictionary<string, string> headers, byte[] body)
        {
            var newTimeout = new Timeout(headers, body, approximateDueTime.UtcDateTime);
            
            _log.Debug("Deferring message with ID {0} until {1} (doc ID {2})", headers.GetValue(Headers.MessageId), approximateDueTime, newTimeout.Id);

            using (var session = _documentStore.OpenAsyncSession())
            {
                await session.StoreAsync(newTimeout);
                await session.SaveChangesAsync();
            }
        }

        /// <summary>
        /// Gets due messages as of now, given the approximate due time that they were stored with when <see cref="ITimeoutManager.Defer"/> was called
        /// </summary>
        public async Task<DueMessagesResult> GetDueMessages()
        {
            var now = _rebusTime.Now.UtcDateTime;

            var session = _documentStore.OpenSession();

            var timeouts = session.Query<Timeout>()
                .Customize(x => x.WaitForNonStaleResults(TimeSpan.FromSeconds(10)))
                .Where(x => x.DueTimeUtc <= now)
                .ToList();

            var dueMessages = timeouts
                .Select(timeout => new DueMessage(timeout.Headers, timeout.Body, async () =>
                {
                    var command = new DeleteCommandData(timeout.Id, session.Advanced.GetChangeVectorFor(timeout));

                    session.Advanced.Defer(command);
                }));

            return new DueMessagesResult(dueMessages, async () =>
            {
                try
                {
                    session.SaveChanges();
                }
                finally
                {
                    session.Dispose();
                }
            });
        }
    }
}