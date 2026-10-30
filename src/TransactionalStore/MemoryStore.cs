using System.Collections.ObjectModel;

namespace TransactionalStore;

public enum TransactionStatus { Active, Committed, Aborted }

public sealed record CommitResult(bool Succeeded, long Version, IReadOnlyList<string> ConflictingKeys, bool PredicateConflict);

internal readonly record struct Cell(string? Value, long Version);

/// <summary>Optimistic serializable transactions over immutable string values.</summary>
public sealed class MemoryStore
{
    private readonly object gate = new();
    private readonly Dictionary<string, Cell> cells = new(StringComparer.Ordinal);
    private long version;

    public Transaction Begin()
    {
        lock (gate)
            return new Transaction(this, new Dictionary<string, Cell>(cells, StringComparer.Ordinal), version);
    }

    internal CommitResult Commit(Transaction tx)
    {
        lock (gate)
        {
            var conflicts = tx.ReadKeys.Concat(tx.Writes.Keys).Distinct(StringComparer.Ordinal)
                .Where(key => VersionOf(cells, key) != VersionOf(tx.Snapshot, key))
                .Order(StringComparer.Ordinal).ToArray();
            bool predicateConflict = tx.Scanned && version != tx.SnapshotVersion;
            if (conflicts.Length != 0 || predicateConflict)
                return new(false, version, Array.AsReadOnly(conflicts), predicateConflict);

            // Calculate the complete next generation before changing any cell.
            // Deleted keys retain a version, preventing absent -> present -> absent ABA.
            if (tx.Writes.Count != 0)
            {
                long nextVersion = checked(version + 1);
                foreach (var (key, value) in tx.Writes)
                    cells[key] = new Cell(value, nextVersion);
                version = nextVersion;
            }
            return new(true, version, Array.Empty<string>(), false);
        }
    }

    private static long VersionOf(IReadOnlyDictionary<string, Cell> source, string key) =>
        source.TryGetValue(key, out var cell) ? cell.Version : 0;
}

/// <summary>A transaction is single-owner; separate transactions may run concurrently.</summary>
public sealed class Transaction : IDisposable
{
    private readonly MemoryStore owner;
    internal Dictionary<string, Cell> Snapshot { get; }
    internal HashSet<string> ReadKeys { get; } = new(StringComparer.Ordinal);
    internal Dictionary<string, string?> Writes { get; } = new(StringComparer.Ordinal);
    internal bool Scanned { get; private set; }
    public long SnapshotVersion { get; }
    public TransactionStatus Status { get; private set; }

    internal Transaction(MemoryStore owner, Dictionary<string, Cell> snapshot, long version)
    {
        this.owner = owner;
        Snapshot = snapshot;
        SnapshotVersion = version;
    }

    public bool TryGet(string key, out string? value)
    {
        EnsureActive();
        ValidateKey(key);
        ReadKeys.Add(key);
        if (Writes.TryGetValue(key, out value)) return value is not null;
        value = Snapshot.TryGetValue(key, out var cell) ? cell.Value : null;
        return value is not null;
    }

    public void Set(string key, string value)
    {
        EnsureActive();
        ValidateKey(key);
        ArgumentNullException.ThrowIfNull(value);
        Writes[key] = value;
    }

    public void Delete(string key)
    {
        EnsureActive();
        ValidateKey(key);
        Writes[key] = null;
    }

    /// <summary>Ordinal prefix scan with conservative whole-store predicate validation.</summary>
    public IReadOnlyDictionary<string, string> Scan(string prefix = "")
    {
        EnsureActive();
        ArgumentNullException.ThrowIfNull(prefix);
        Scanned = true;
        var result = new SortedDictionary<string, string>(StringComparer.Ordinal);
        foreach (var (key, cell) in Snapshot)
            if (cell.Value is not null && key.StartsWith(prefix, StringComparison.Ordinal)) result[key] = cell.Value;
        foreach (var (key, value) in Writes)
        {
            if (!key.StartsWith(prefix, StringComparison.Ordinal)) continue;
            if (value is null) result.Remove(key);
            else result[key] = value;
        }
        return new ReadOnlyDictionary<string, string>(result);
    }

    public CommitResult TryCommit()
    {
        EnsureActive();
        try
        {
            var result = owner.Commit(this);
            Status = result.Succeeded ? TransactionStatus.Committed : TransactionStatus.Aborted;
            return result;
        }
        catch
        {
            Status = TransactionStatus.Aborted;
            throw;
        }
    }

    public void Dispose()
    {
        if (Status == TransactionStatus.Active) Status = TransactionStatus.Aborted;
    }

    private void EnsureActive()
    {
        if (Status != TransactionStatus.Active) throw new InvalidOperationException($"Transaction is {Status}.");
    }

    private static void ValidateKey(string key)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
    }
}
