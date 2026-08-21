package netbench.backend;

import application.wan.bdd.verifier.common.GcflobddEngine;
import netbench.Fields;
import netbench.PacketDd;

/**
 * GCFLOBDD through {@code gcflobdd-jni}.
 *
 * <p>{@code createVar()} hands out flat indices 0..103 in sequence, and all
 * three grammars keep the same 104-leaf order, so allocating in the five-tuple
 * order (MSB-first within each field) lands every field on the grammar subtree
 * meant for it.
 */
public final class GcflobddBackend implements PacketDd {

    private final GcflobddEngine e;
    private final int[][] vars = new int[Fields.COUNT][];
    private final String label;

    public GcflobddBackend(int config) {
        e = new GcflobddEngine(config);
        if (e.numVars() != Fields.TOTAL_BITS) {
            throw new IllegalStateException(
                    "grammar has " + e.numVars() + " variables, expected " + Fields.TOTAL_BITS);
        }
        for (int f = 0; f < Fields.COUNT; f++) {
            vars[f] = new int[Fields.WIDTH[f]];
            for (int bit = 0; bit < Fields.WIDTH[f]; bit++) {
                vars[f][bit] = e.createVar();
            }
        }
        label = switch (config) {
            case GcflobddEngine.CONFIG_FIELD_GROUPED -> "gcflobdd/field-grouped";
            case GcflobddEngine.CONFIG_ALIGNED_BALANCED -> "gcflobdd/aligned-balanced";
            case GcflobddEngine.CONFIG_ALIGNED_BALANCED_SHARED -> "gcflobdd/aligned-balanced-shared";
            default -> "gcflobdd/" + config;
        };
    }

    @Override public int var(int field, int bit) { return vars[field][bit]; }
    @Override public int and(int a, int b)       { return e.and(a, b); }
    @Override public int or(int a, int b)        { return e.or(a, b); }
    @Override public int not(int a)              { return e.not(a); }
    @Override public int diff(int a, int b)      { return e.diff(a, b); }
    @Override public int ref(int a)              { return e.ref(a); }
    @Override public void deref(int a)           { e.deref(a); }
    @Override public double satCount(int a)      { return e.satCount(a); }
    @Override public long engineNodes()          { return e.nodeCount(); }
    @Override public long engineBytes()          { return e.memoryUsage(); }
    @Override public int diagramNodes(int a)     { return e.diagramNodes(a); }
    @Override public int diagramEdges(int a)     { return e.diagramEdges(a); }

    /** Size under the reference C++ CFLOBDD convention, as `results/` reports it. */
    public int convTotal(int a)                  { return e.convTotal(a); }

    @Override public void gc()                   { e.gc(); }
    @Override public String name()               { return label; }
    @Override public void close()                { e.close(); }
}
