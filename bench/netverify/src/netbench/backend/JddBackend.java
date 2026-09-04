package netbench.backend;

import jdd.bdd.BDD;
import netbench.Fields;
import netbench.PacketDd;

/**
 * Plain BDD baseline on JDD, the engine NDD itself uses for its edge labels and
 * for its own BDD comparison arm.
 *
 * <p>Variables are created in the five-tuple order, MSB-first within each field,
 * which is exactly what NDD's pure-BDD wrapper does
 * ({@code application/wan/bdd/verifier/common/BDDACLWrapper.DeclareVars} walks
 * {@code i = bits-1 .. 0} over an LSB-indexed array, so the MSB gets the lowest
 * BDD variable index). Global order top to bottom is therefore
 * srcIP[31..0], dstIP[31..0], srcPort[15..0], dstPort[15..0], protocol[7..0].
 */
public final class JddBackend implements PacketDd {

    private final BDD bdd;
    private final int[][] vars = new int[Fields.COUNT][];

    public JddBackend(int nodes, int cache) {
        bdd = new BDD(nodes, cache);
        for (int f = 0; f < Fields.COUNT; f++) {
            vars[f] = new int[Fields.WIDTH[f]];
            for (int bit = 0; bit < Fields.WIDTH[f]; bit++) {
                vars[f][bit] = bdd.createVar();
            }
        }
    }

    @Override public int var(int field, int bit) { return vars[field][bit]; }
    @Override public int and(int a, int b)       { return bdd.and(a, b); }
    @Override public int or(int a, int b)        { return bdd.or(a, b); }
    @Override public int not(int a)              { return bdd.not(a); }

    @Override
    public int diff(int a, int b) {
        int nb = bdd.ref(bdd.not(b));
        int r = bdd.and(a, nb);
        bdd.deref(nb);
        return r;
    }

    @Override public int ref(int a)          { return bdd.ref(a); }
    @Override public void deref(int a)       { bdd.deref(a); }
    @Override public double satCount(int a)  { return bdd.satCount(a); }

    /** JDD keeps its live-node count private; only per-diagram counts are public. */
    @Override public long engineNodes()      { return -1; }
    @Override public long engineBytes()      { return bdd.getMemoryUsage(); }
    @Override public int diagramNodes(int a) { return bdd.nodeCount(a); }
    @Override public int diagramEdges(int a) { return -1; }
    @Override public void gc()               { bdd.gc(); }
    @Override public String name()           { return "jdd-bdd"; }
    @Override public void close()            { bdd.cleanup(); }

    public BDD raw() { return bdd; }
}
