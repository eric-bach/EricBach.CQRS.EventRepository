namespace EricBach.CQRS.EventRepository.Snapshots
{
    public interface ISnapshot
    {
        Snapshot GetSnapshot();
        void SetSnapshot(Snapshot snapshot);
    }
}
