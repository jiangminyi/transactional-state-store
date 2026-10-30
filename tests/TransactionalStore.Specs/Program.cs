using TransactionalStore;

var specs = new (string Name, Action Run)[]
{
    ("read your writes and deletes", () => {
        var s = new MemoryStore(); using var t = s.Begin(); t.Set("a", "1"); Eq("1", Get(t, "a"));
        t.Delete("a"); Yes(!t.TryGet("a", out _)); Yes(t.TryCommit().Succeeded); Eq(0, s.Begin().Scan().Count);
    }),
    ("repeatable snapshot survives concurrent commit", () => {
        var s = Seed(("a", "1")); using var old = s.Begin(); Put(s, "a", "2"); Eq("1", Get(old, "a"));
        Yes(!old.TryCommit().Succeeded);
    }),
    ("blind write conflict", () => {
        var s = new MemoryStore(); using var a = s.Begin(); using var b = s.Begin();
        a.Set("x", "a"); b.Set("x", "b"); Yes(a.TryCommit().Succeeded); var r = b.TryCommit();
        Yes(!r.Succeeded); Eq("x", r.ConflictingKeys.Single()); Eq("a", Get(s.Begin(), "x"));
    }),
    ("disjoint writes both commit", () => {
        var s = new MemoryStore(); using var a = s.Begin(); using var b = s.Begin();
        a.Set("a", "1"); b.Set("b", "2"); Yes(a.TryCommit().Succeeded); Yes(b.TryCommit().Succeeded);
    }),
    ("write skew rejected by read set validation", () => {
        var s = Seed(("alice", "on"), ("bob", "on")); using var a = s.Begin(); using var b = s.Begin();
        Eq("on", Get(a, "bob")); Eq("on", Get(b, "alice")); a.Set("alice", "off"); b.Set("bob", "off");
        Yes(a.TryCommit().Succeeded); Yes(!b.TryCommit().Succeeded); Eq("on", Get(s.Begin(), "bob"));
    }),
    ("absent key read conflicts with insert", () => {
        var s = new MemoryStore(); using var a = s.Begin(); Yes(!a.TryGet("x", out _)); Put(s, "x", "1");
        a.Set("other", "2"); Yes(!a.TryCommit().Succeeded); Yes(!s.Begin().TryGet("other", out _));
    }),
    ("tombstone detects absent present absent ABA", () => {
        var s = new MemoryStore(); using var a = s.Begin(); Yes(!a.TryGet("x", out _)); Put(s, "x", "1");
        using var d = s.Begin(); d.Delete("x"); Yes(d.TryCommit().Succeeded); Yes(!a.TryCommit().Succeeded);
    }),
    ("same value update still invalidates readers", () => {
        var s = Seed(("x", "1")); using var a = s.Begin(); Get(a, "x"); Put(s, "x", "1"); Yes(!a.TryCommit().Succeeded);
    }),
    ("failed batch leaves every key unchanged", () => {
        var s = Seed(("a", "0"), ("b", "0")); using var a = s.Begin(); a.Set("a", "1"); a.Set("b", "1");
        Put(s, "b", "2"); Yes(!a.TryCommit().Succeeded); Eq("0", Get(s.Begin(), "a")); Eq("2", Get(s.Begin(), "b"));
    }),
    ("scan prevents phantom inserts", () => {
        var s = new MemoryStore(); using var a = s.Begin(); Eq(0, a.Scan("job/").Count); Put(s, "job/new", "1");
        var r = a.TryCommit(); Yes(!r.Succeeded && r.PredicateConflict);
    }),
    ("scan conservative conflicts are documented", () => {
        var s = new MemoryStore(); using var a = s.Begin(); a.Scan("job/"); Put(s, "unrelated", "1"); Yes(a.TryCommit().PredicateConflict);
    }),
    ("scan merges writes in ordinal order", () => {
        var s = Seed(("p/a", "1"), ("p/b", "2"), ("q", "3")); using var t = s.Begin();
        t.Delete("p/a"); t.Set("p/c", "4"); t.Set("p/b", "5"); var rows = t.Scan("p/");
        Eq("p/b,p/c", string.Join(',', rows.Keys)); Eq("5", rows["p/b"]); Yes(t.TryCommit().Succeeded);
    }),
    ("empty commit does not advance generation", () => {
        var s = new MemoryStore(); using var t = s.Begin(); Eq(0L, t.TryCommit().Version); Eq(0L, s.Begin().SnapshotVersion);
    }),
    ("terminal transactions reject reuse", () => {
        var s = new MemoryStore(); using var t = s.Begin(); t.TryCommit(); Throws<InvalidOperationException>(() => t.Set("a", "1"));
        Throws<InvalidOperationException>(() => t.TryCommit()); using var d = s.Begin(); d.Dispose(); Throws<InvalidOperationException>(() => d.Scan());
    }),
    ("abort discards writes and is idempotent", () => {
        var s = new MemoryStore(); var t = s.Begin(); t.Set("x", "1"); t.Dispose(); t.Dispose();
        Eq(TransactionStatus.Aborted, t.Status); Yes(!s.Begin().TryGet("x", out _));
    }),
    ("keys are case sensitive and arguments validated", () => {
        var s = new MemoryStore(); using var t = s.Begin(); t.Set("a", "1"); t.Set("A", "2"); Eq(2, t.Scan().Count);
        Throws<ArgumentException>(() => t.Set("", "x")); Throws<ArgumentNullException>(() => t.Set("x", null!));
        Throws<ArgumentNullException>(() => t.Scan(null!)); Eq(2, t.Scan().Count);
    }),
    ("one winner among simultaneous conflicting writers", () => {
        var s = new MemoryStore(); var transactions = Enumerable.Range(0, 32).Select(i => { var t = s.Begin(); t.Set("x", i.ToString()); return t; }).ToArray();
        int successes = 0; Parallel.ForEach(transactions, t => { if (t.TryCommit().Succeeded) Interlocked.Increment(ref successes); }); Eq(1, successes);
    }),
    ("concurrent increment with explicit retry loses no updates", () => {
        var s = Seed(("count", "0")); Parallel.For(0, 4, _ => {
            for (int n = 0; n < 100; n++) {
                bool done = false;
                for (int attempt = 0; attempt < 10000 && !done; attempt++) {
                    using var t = s.Begin(); t.Set("count", (int.Parse(Get(t, "count")) + 1).ToString()); done = t.TryCommit().Succeeded;
                }
                Yes(done);
            }
        }); Eq("400", Get(s.Begin(), "count"));
    }),
    ("concurrent snapshots observe atomic transfers", () => {
        var s = Seed(("left", "1000"), ("right", "0")); var writer = Task.Run(() => {
            for (int i = 0; i < 500; i++) { using var t = s.Begin(); t.Set("left", (999-i).ToString()); t.Set("right", (i+1).ToString()); Yes(t.TryCommit().Succeeded); }
        });
        for (int i = 0; i < 1000; i++) { using var t = s.Begin(); Eq(1000, int.Parse(Get(t, "left")) + int.Parse(Get(t, "right"))); }
        writer.GetAwaiter().GetResult();
    }),
    ("seeded sequential operations match dictionary oracle", () => {
        var s = new MemoryStore(); var model = new Dictionary<string, string>(StringComparer.Ordinal); var random = new Random(5017);
        for (int i = 0; i < 1000; i++) {
            using var t = s.Begin(); var next = new Dictionary<string, string>(model, StringComparer.Ordinal);
            for (int j = 0; j < 5; j++) {
                var k = "k" + random.Next(20); if (random.Next(3) == 0) { t.Delete(k); next.Remove(k); }
                else { var v = random.Next(1000).ToString(); t.Set(k, v); next[k] = v; }
            }
            EqualMaps(next, t.Scan());
            if (random.Next(4) != 0) { Yes(t.TryCommit().Succeeded); model = next; }
            EqualMaps(model, s.Begin().Scan());
        }
    })
};
int failures = 0;
foreach (var (name, run) in specs) { try { run(); Console.WriteLine($"PASS {name}"); } catch (Exception ex) { failures++; Console.Error.WriteLine($"FAIL {name}: {ex}"); } }
Console.WriteLine($"{specs.Length - failures}/{specs.Length} specifications passed.");
return failures == 0 ? 0 : 1;

static void Yes(bool condition) { if (!condition) throw new Exception("Assertion failed."); }
static void Eq<T>(T expected, T actual) { if (!EqualityComparer<T>.Default.Equals(expected, actual)) throw new Exception($"Expected {expected}, got {actual}."); }
static string Get(Transaction t, string key) { Yes(t.TryGet(key, out var value)); return value!; }
static void Put(MemoryStore s, string key, string value) { using var t = s.Begin(); t.Set(key, value); Yes(t.TryCommit().Succeeded); }
static MemoryStore Seed(params (string Key, string Value)[] values) { var s = new MemoryStore(); using var t = s.Begin(); foreach (var (k,v) in values) t.Set(k,v); Yes(t.TryCommit().Succeeded); return s; }
static void Throws<T>(Action action) where T : Exception { try { action(); } catch (T) { return; } throw new Exception($"Expected {typeof(T).Name}."); }
static void EqualMaps(IReadOnlyDictionary<string,string> expected, IReadOnlyDictionary<string,string> actual) { Eq(expected.Count, actual.Count); foreach (var (k,v) in expected) { Yes(actual.TryGetValue(k, out var found)); Eq(v, found); } }
