using System;
using EricBach.CQRS.Aggregate;

namespace EricBach.CQRS.EventRepository
{
    public interface IEventRepository<T> where T : AggregateRoot, new()
    {
        void Save(AggregateRoot aggregate, int expectedVersion);

        T GetById(Guid id);
        bool VerifyAggregateExists(Guid id);
        
        void DeleteAll();
    }
}
