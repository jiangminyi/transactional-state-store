using TransactionalStore;

var store = new MemoryStore();
using (var seed = store.Begin()) { seed.Set("doctor/alice", "on"); seed.Set("doctor/bob", "on"); seed.TryCommit(); }
using var alice = store.Begin();
using var bob = store.Begin();
alice.TryGet("doctor/bob", out _);
bob.TryGet("doctor/alice", out _);
alice.Set("doctor/alice", "off");
bob.Set("doctor/bob", "off");
Console.WriteLine($"Alice commit: {alice.TryCommit().Succeeded}");
var rejected = bob.TryCommit();
Console.WriteLine($"Bob commit: {rejected.Succeeded}; conflict: {string.Join(',', rejected.ConflictingKeys)}");
using var view = store.Begin();
foreach (var (key,value) in view.Scan("doctor/")) Console.WriteLine($"{key} = {value}");
