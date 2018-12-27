using System.Linq;
using Raven.Client.Documents.Indexes;

namespace Rebus.RavenDb.Timouts
{
    class TimeoutIndex : AbstractIndexCreationTask<Timeout>
    {
        /// <summary>
        /// Defines the index
        /// </summary>
        public TimeoutIndex()
        {
            Map = timeouts => from timeout in timeouts
                              select new
                              {
                                  Id = timeout.Id,
                                  DueTimeUtc = timeout.DueTimeUtc
                              };
        }
    }
}