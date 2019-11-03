using System;
using System.Collections.Generic;
using System.Linq;
using EricBach.CQRS.Aggregate;
using EricBach.CQRS.Events;
using EricBach.CQRS.Exceptions;
using EricBach.LambdaLogger;
using IEventStore = EricBach.CQRS.EventRepository.EventStore.IEventStore;
using ISnapshot = EricBach.CQRS.EventRepository.Snapshots.ISnapshot;
using Snapshot = EricBach.CQRS.EventRepository.Snapshots.Snapshot;

namespace EricBach.CQRS.EventRepository
{
    public class EventRepository<T> : IEventRepository<T> where T : AggregateRoot, new()
    {
        private readonly IEventStore _eventStore;
        private static readonly object _lockStorage = new object();

        public EventRepository(IEventStore eventStore)
        {
            _eventStore = eventStore;
        }

        public void Save(AggregateRoot aggregate, int expectedVersion)
        {
            Logger.Log($"Preparing to write aggregate {aggregate.Id} to repository");

            if (aggregate.GetUncommittedChanges().Any())
            {
                lock (_lockStorage)
                {
                    var item = new T();

                    if (expectedVersion != 0)
                    {
                        item = GetById(aggregate.Id);
                        if (item.Version != expectedVersion)
                        {
                            throw new ConcurrencyException($"Aggregate {item.Id} has been previous modified");
                        }
                    }

                    _eventStore.SaveAsync(aggregate).Wait();
                }
            }
        }

        public T GetById(Guid id)
        {
            IEnumerable<Event> events;

            Logger.Log($"Looking for aggregate {id} in EventStore");

            var snapshot = _eventStore.GetSnapshot<Snapshot>(id);

            if (snapshot != null)
            {
                events = _eventStore.GetEventsAsync(id).Result.Where(e => e.Version >= snapshot.Version);
            }
            else
            {
                events = _eventStore.GetEventsAsync(id).Result;
            }

            Logger.Log($"Found {events.Count()} events for {id} in EventStore");

            var obj = new T();
            if (snapshot != null)
            {
                ((ISnapshot)obj).SetSnapshot(snapshot);
            }

            obj.LoadFromHistory(events);
            return obj;
        }

        public bool VerifyAggregateExists(Guid id)
        {
            Logger.Log($"Verifying aggregate {id} exists in EventStore");

            return _eventStore.VerifyAggregateExistsAsync(id).Result;
        }

        public void DeleteAll()
        {
            _eventStore.DeleteAllAsync();
        }
    }
}
