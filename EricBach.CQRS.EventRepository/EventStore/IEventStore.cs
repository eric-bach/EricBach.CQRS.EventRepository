using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using EricBach.CQRS.Aggregate;
using EricBach.CQRS.EventRepository.Snapshots;
using EricBach.CQRS.Events;

namespace EricBach.CQRS.EventRepository.EventStore
{
    public interface IEventStore
    {
        Task<IEnumerable<Event>> GetAllEventsAsync();
        Task<IEnumerable<Event>> GetEventsAsync(Guid aggregateId);
        Task<bool> VerifyAggregateExistsAsync(Guid aggregateId);

        Task SaveAsync(AggregateRoot aggregate);

        Task DeleteAllAsync();

        T GetSnapshot<T>(Guid aggregateId) where T : Snapshot;
        Task SaveSnapshotAsync(Snapshot snapshot);
    }
}
