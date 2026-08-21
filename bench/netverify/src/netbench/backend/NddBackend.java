package netbench.backend;

import netbench.Fields;
import netbench.PacketDd;
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

    /** NDD counts nodes globally only; there is no per-diagram walk. */
    @Override public int diagramNodes(int a)     { return -1; }
    @Override public int diagramEdges(int a)     { return -1; }

    @Override public void gc()                   { NDD.gc(); }
    @Override public String name()               { return "ndd"; }
    @Override public void close()                { /* static engine, nothing to free */ }
}
