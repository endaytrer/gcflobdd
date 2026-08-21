package netbench;

/**
 * The one decision-diagram interface every backend implements, so jdd-BDD, NDD
 * and GCFLOBDD run byte-identical inputs through identical driver code.
 *
 * <p>Handles are {@code int}s and must be <b>canonical</b>: two handles are
 * equal iff they denote the same Boolean function. The atomic-predicate kernel
 * depends on that, comparing atoms with {@code ==}.
 *
 * <p>Constants are {@code 0 = false}, {@code 1 = true} on all three engines.
 */
public interface PacketDd extends AutoCloseable {

    int FALSE = 0;
    int TRUE = 1;

    /** Positive literal for bit {@code bit} (0 = MSB) of {@code field}. */
    int var(int field, int bit);

    int and(int a, int b);
    int or(int a, int b);
    int not(int a);

    /** {@code a AND NOT b}. The hot operation in atomic-predicate splitting. */
    int diff(int a, int b);

    int ref(int a);
    void deref(int a);

    default boolean isFalse(int a) { return a == FALSE; }
    default boolean isTrue(int a)  { return a == TRUE; }

    double satCount(int a);

    /**
     * Live nodes in the engine's primary table, or -1 where the engine does not
     * expose one (JDD keeps its live count private). Not comparable across
     * engine families -- a GCFLOBDD node and a BDD node are not the same thing.
     */
    long engineNodes();

    /**
     * Live nodes in a secondary table, or -1. NDD is the case that needs this:
     * its size is NDD nodes <i>plus</i> the label BDDs underneath them, which is
     * how NDD's own {@code nqueens_metrics.csv} splits the column.
     */
    default long engineAuxNodes() { return -1; }

    /** Engine heap estimate in bytes, or -1 if unsupported. */
    default long engineBytes() { return -1; }

    /** Nodes in <i>this</i> diagram, or -1 if the engine cannot report it. */
    int diagramNodes(int a);

    /** Edges in this diagram, or -1 if unsupported. */
    int diagramEdges(int a);

    void gc();

    /** Short identifier used in the CSV's {@code impl} column. */
    String name();

    @Override
    void close();
}
