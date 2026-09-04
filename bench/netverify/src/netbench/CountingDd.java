package netbench;

/**
 * A {@link PacketDd} that counts what the driver asks of it, so a stage's time
 * can be divided by the operations it actually performed.
 *
 * <p>Delegation only: it changes no result and adds one increment per call, so
 * the timings it enables are the engine's own to within that.
 */
public final class CountingDd implements PacketDd {

    private final PacketDd inner;
    public long ands, ors, nots, diffs;

    public CountingDd(PacketDd inner) { this.inner = inner; }

    /** Conjunction-like operations -- the atomic-predicate loop is all of these. */
    public long binaryOps() { return ands + ors + diffs; }

    public void reset() { ands = ors = nots = diffs = 0; }

    @Override public int var(int f, int b)  { return inner.var(f, b); }
    @Override public int and(int a, int b)  { ands++;  return inner.and(a, b); }
    @Override public int or(int a, int b)   { ors++;   return inner.or(a, b); }
    @Override public int not(int a)         { nots++;  return inner.not(a); }
    @Override public int diff(int a, int b) { diffs++; return inner.diff(a, b); }
    @Override public int ref(int a)         { return inner.ref(a); }
    @Override public void deref(int a)      { inner.deref(a); }
    @Override public boolean isFalse(int a) { return inner.isFalse(a); }
    @Override public boolean isTrue(int a)  { return inner.isTrue(a); }
    @Override public double satCount(int a) { return inner.satCount(a); }
    @Override public long engineNodes()     { return inner.engineNodes(); }
    @Override public long engineAuxNodes()  { return inner.engineAuxNodes(); }
    @Override public long engineBytes()     { return inner.engineBytes(); }
    @Override public int diagramNodes(int a){ return inner.diagramNodes(a); }
    @Override public int diagramEdges(int a){ return inner.diagramEdges(a); }
    @Override public void gc()              { inner.gc(); }
    @Override public String name()          { return inner.name(); }
    @Override public void close() { inner.close(); }

    public PacketDd unwrap() { return inner; }
}
