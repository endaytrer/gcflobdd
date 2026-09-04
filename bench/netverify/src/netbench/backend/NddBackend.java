package netbench.backend;

import netbench.Fields;
import netbench.PacketDd;
import com.carrotsearch.hppc.IntArrayDeque;
import com.carrotsearch.hppc.IntHashSet;
import org.ants.jndd.diagram.NDD;

/**
 * NDD (NSDI '25), the reference this whole benchmark exists to meet.
 *
 * <p>Fields are declared in the five-tuple order so field index == NDD level;
 * {@code NDD.getVar(field, bit)} takes bit 0 as the field's MSB, matching
 * {@link Fields}. {@code generateFields()} then right-aligns one shared variable
 * pool of width {@code max(32)} per backend, so the 16- and 8-bit fields reuse
 * the top of the same pool the IP fields use -- NDD's label-sharing design.
 *
 * <p>NDD's API is all-static, so only one NDD backend can be live per JVM.
 */
public final class NddBackend implements PacketDd {

    private static boolean initialised = false;
    private final int[][] vars = new int[Fields.COUNT][];

    public NddBackend(int nddTable, int bddTable, int bddCache) {
        if (initialised) {
            throw new IllegalStateException(
                    "NDD is all-static: only one NddBackend per JVM");
        }
        initialised = true;

        NDD.initNDD(nddTable, bddTable, bddCache);
        for (int f = 0; f < Fields.COUNT; f++) {
            NDD.declareField(Fields.WIDTH[f]);
        }
        NDD.generateFields();

        for (int f = 0; f < Fields.COUNT; f++) {
            vars[f] = new int[Fields.WIDTH[f]];
            for (int bit = 0; bit < Fields.WIDTH[f]; bit++) {
                vars[f][bit] = NDD.getVar(f, bit);
            }
        }
    }

    @Override public int var(int field, int bit) { return vars[field][bit]; }
    @Override public int and(int a, int b)       { return NDD.and(a, b); }
    @Override public int or(int a, int b)        { return NDD.or(a, b); }
    @Override public int not(int a)              { return NDD.not(a); }
    @Override public int diff(int a, int b)      { return NDD.diff(a, b); }
    @Override public int ref(int a)              { return NDD.ref(a); }
    @Override public void deref(int a)           { NDD.deref(a); }
    @Override public double satCount(int a)      { return NDD.satCount(a); }
    @Override public long engineNodes()          { return NDD.getNodeCount(); }

    /** The label decision diagrams under the NDD nodes -- NDD's second size axis. */
    @Override public long engineAuxNodes()       { return NDD.getLabelNodeCount(); }

    /**
     * Nodes in <i>this</i> NDD diagram. NDD exposes no walk of its own, but its
     * node and edge accessors are public, so one is written here: a BFS over
     * distinct node ids, terminals excluded, matching what
     * {@code jdd.bdd.BDD.nodeCount} counts for the BDD backend.
     *
     * <p>This counts NDD nodes only. Each edge additionally carries a label
     * decision diagram; {@link #labelNodes} counts those, deduplicated across
     * the whole diagram, and the two together are NDD's size.
     */
    @Override public int diagramNodes(int a) {
        if (NDD.isTerminal(a)) return 0;
        IntHashSet seen = new IntHashSet();
        IntArrayDeque queue = new IntArrayDeque();
        seen.add(a); queue.addLast(a);
        int count = 0;
        while (!queue.isEmpty()) {
            int n = queue.removeFirst();
            count++;
            int start = NDD.getEdgeStart(n), num = NDD.getEdgeCount(n);
            for (int i = 0; i < num; i++) {
                int t = NDD.getEdgeTarget(start + i);
                if (!NDD.isTerminal(t) && seen.add(t)) queue.addLast(t);
            }
        }
        return count;
    }

    @Override public int diagramEdges(int a) {
        if (NDD.isTerminal(a)) return 0;
        IntHashSet seen = new IntHashSet();
        IntArrayDeque queue = new IntArrayDeque();
        seen.add(a); queue.addLast(a);
        int edges = 0;
        while (!queue.isEmpty()) {
            int n = queue.removeFirst();
            int start = NDD.getEdgeStart(n), num = NDD.getEdgeCount(n);
            edges += num;
            for (int i = 0; i < num; i++) {
                int t = NDD.getEdgeTarget(start + i);
                if (!NDD.isTerminal(t) && seen.add(t)) queue.addLast(t);
            }
        }
        return edges;
    }

    /**
     * Label-BDD nodes under this diagram, deduplicated across every edge --
     * NDD's second size axis. Requires the default BDD label mode; returns -1
     * otherwise.
     */
    public int labelNodes(int a) {
        if (NDD.getLabelMode() != NDD.LabelMode.BDD) return -1;
        if (NDD.isTerminal(a)) return 0;
        jdd.bdd.BDD bdd = NDD.getBDDEngine();
        IntHashSet seenNodes = new IntHashSet();
        IntArrayDeque queue = new IntArrayDeque();
        IntHashSet seenLabelNodes = new IntHashSet();
        IntArrayDeque labelQueue = new IntArrayDeque();
        seenNodes.add(a); queue.addLast(a);
        while (!queue.isEmpty()) {
            int n = queue.removeFirst();
            int start = NDD.getEdgeStart(n), num = NDD.getEdgeCount(n);
            for (int i = 0; i < num; i++) {
                int label = NDD.getEdgeLabel(start + i);
                if (label > 1 && seenLabelNodes.add(label)) labelQueue.addLast(label);
                int t = NDD.getEdgeTarget(start + i);
                if (!NDD.isTerminal(t) && seenNodes.add(t)) queue.addLast(t);
            }
        }
        int count = 0;
        while (!labelQueue.isEmpty()) {
            int b = labelQueue.removeFirst();
            count++;
            int lo = bdd.getLow(b), hi = bdd.getHigh(b);
            if (lo > 1 && seenLabelNodes.add(lo)) labelQueue.addLast(lo);
            if (hi > 1 && seenLabelNodes.add(hi)) labelQueue.addLast(hi);
        }
        return count;
    }

    /** Fields this diagram actually branches on -- the ones it does not skip. */
    public int fieldsTouched(int a) {
        if (NDD.isTerminal(a)) return 0;
        IntHashSet seen = new IntHashSet();
        IntHashSet fields = new IntHashSet();
        IntArrayDeque queue = new IntArrayDeque();
        seen.add(a); queue.addLast(a);
        while (!queue.isEmpty()) {
            int n = queue.removeFirst();
            fields.add(NDD.getField(n));
            int start = NDD.getEdgeStart(n), num = NDD.getEdgeCount(n);
            for (int i = 0; i < num; i++) {
                int t = NDD.getEdgeTarget(start + i);
                if (!NDD.isTerminal(t) && seen.add(t)) queue.addLast(t);
            }
        }
        return fields.size();
    }

    @Override public void gc()                   { NDD.gc(); }
    @Override public String name()               { return "ndd"; }
    @Override public void close()                { /* static engine, nothing to free */ }
}
