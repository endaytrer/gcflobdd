package netbench;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import netbench.Dataset.FibEntry;
import netbench.Dataset.Link;
import netbench.Dataset.Port;

/**
 * The network-verification kernels, written once against {@link PacketDd} so
 * every engine executes the identical sequence of decision-diagram operations.
 *
 * <p>Three stages, matching the APKeep / AP-Verifier pipeline:
 * forwarding and ACL predicates, then atomic predicates, then reachability.
 */
public final class Verifier {

    private final PacketDd dd;
    private final Encoder enc;
    private final Dataset data;

    /** Forwarding predicate of each device/port, under longest-prefix match. */
    public final Map<Port, Integer> portPredicate = new LinkedHashMap<>();
    /** Permitted set of each named ACL. */
    public final Map<String, Integer> aclPredicate = new LinkedHashMap<>();

    public Verifier(PacketDd dd, Dataset data) {
        this.dd = dd;
        this.enc = new Encoder(dd);
        this.data = data;
    }

    // --- stage 1: predicates ------------------------------------------------

    /**
     * Longest-prefix match, exactly as {@code BDDACLWrapper.getfwdbdds} does it:
     * walk prefix lengths from 32 down to 0 and give each entry only what no
     * longer prefix on the same device already claimed.
     */
    public void buildForwarding() {
        Map<String, List<FibEntry>> byDevice = new LinkedHashMap<>();
        for (FibEntry e : data.fib) {
            byDevice.computeIfAbsent(e.device(), x -> new ArrayList<>()).add(e);
        }
        for (Map.Entry<String, List<FibEntry>> dev : byDevice.entrySet()) {
            List<FibEntry> entries = new ArrayList<>(dev.getValue());
            entries.sort(Comparator.comparingInt(FibEntry::len).reversed());

            int claimed = dd.ref(PacketDd.FALSE);
            Map<Port, Integer> acc = new LinkedHashMap<>();
            for (FibEntry e : entries) {
                int hit = enc.prefix(Fields.DST_IP, e.ip(), e.len());   // owned
                int fresh = dd.ref(dd.diff(hit, claimed));
                if (!dd.isFalse(fresh)) {
                    Port p = new Port(e.device(), e.port());
                    Integer old = acc.get(p);
                    if (old == null) {
                        acc.put(p, dd.ref(fresh));
                    } else {
                        int merged = dd.ref(dd.or(old, fresh));
                        dd.deref(old);
                        acc.put(p, merged);
                    }
                }
                int nc = dd.ref(dd.or(claimed, hit));
                dd.deref(claimed);
                claimed = nc;
                dd.deref(fresh);
                dd.deref(hit);
            }
            dd.deref(claimed);
            portPredicate.putAll(acc);
        }
    }

    public void buildAcls() {
        for (Map.Entry<String, List<Rule>> e : data.acls.entrySet()) {
            aclPredicate.put(e.getKey(), enc.permitSet(e.getValue()));   // already owned
        }
    }

    /** Every distinct predicate the atomic-predicate stage has to separate. */
    public List<Integer> allPredicates() {
        List<Integer> out = new ArrayList<>(portPredicate.values());
        out.addAll(aclPredicate.values());
        return out;
    }

    // --- stage 2: atomic predicates -----------------------------------------

    /**
     * Yang and Lam's atomic predicates: the coarsest partition of the header
     * space that every input predicate is a union of.
     *
     * <p>Start from {@code {true}} and split each atom {@code a} by each
     * predicate {@code p} into {@code a AND p} and {@code a AND NOT p}, dropping
     * the empty side. An atom already wholly inside or wholly outside {@code p}
     * is left alone -- detected by canonical handle equality, which is the
     * property this benchmark leans on hardest.
     *
     * <p>Each predicate's set of atoms is accumulated <i>during</i> the split
     * rather than recovered afterwards: when atom {@code i} splits, both halves
     * lie inside every predicate that contained {@code i}, so the bookkeeping is
     * a bit-copy. Recovering the sets afterwards would instead cost
     * {@code |predicates| x |atoms|} conjunctions, which is what makes the naive
     * version quadratic. This is APKeep's bookkeeping.
     */
    public static final class Atoms {
        public final List<Integer> atoms;
        /** Atom indices making up each input predicate, keyed by its handle. */
        public final Map<Integer, BitSet> setOf;

        Atoms(List<Integer> atoms, Map<Integer, BitSet> setOf) {
            this.atoms = atoms;
            this.setOf = setOf;
        }

        public int size() { return atoms.size(); }
    }

    /**
     * Atom count before each predicate was applied, filled in by
     * {@link #atomicPredicates}. The operation count of that loop is the sum of
     * this list plus one extra per split, so its *shape* -- not just its last
     * value -- is what sets how the work scales.
     */
    public final List<Integer> atomTrajectory = new ArrayList<>();

    /**
     * @param cap give up past this many atoms (0 = no cap)
     * @return the atoms and their per-predicate index sets, or {@code null} if
     *         the cap was exceeded
     */
    public Atoms atomicPredicates(List<Integer> predicates, int cap) {
        atomTrajectory.clear();
        List<Integer> atoms = new ArrayList<>();
        atoms.add(dd.ref(PacketDd.TRUE));
        Map<Integer, BitSet> setOf = new LinkedHashMap<>();

        for (int p : predicates) {
            atomTrajectory.add(atoms.size());
            BitSet mine = new BitSet();
            if (p == PacketDd.TRUE) {          // everything is inside
                mine.set(0, atoms.size());
                setOf.put(p, mine);
                continue;
            }
            if (p == PacketDd.FALSE) {          // nothing is
                setOf.put(p, mine);
                continue;
            }

            int n = atoms.size();               // atoms appended below are all
            for (int i = 0; i < n; i++) {       // outside p, so skip them
                int a = atoms.get(i);
                int in = dd.ref(dd.and(a, p));
                if (in == a) { mine.set(i); dd.deref(in); continue; }  // a is inside p
                if (dd.isFalse(in)) { dd.deref(in); continue; }        // a misses p

                int out = dd.ref(dd.diff(a, p));
                dd.deref(a);                              // a leaves the list
                atoms.set(i, in);                         // the inside half stays put
                int j = atoms.size();
                atoms.add(out);                           // the outside half is new
                mine.set(i);
                // Both halves lie inside whatever the old atom did.
                for (BitSet s : setOf.values()) {
                    if (s.get(i)) s.set(j);
                }
            }
            setOf.put(p, mine);
            if (cap > 0 && atoms.size() > cap) return null;
        }
        return new Atoms(atoms, setOf);
    }

    // --- stage 3: reachability ----------------------------------------------

    /** Result of the all-pairs check. */
    public record Reach(int pairs, int atoms) {}

    /**
     * All-pairs reachability between edge ports, carrying sets of atomic
     * predicates rather than packet sets -- the reason atomic predicates exist.
     * A packet enters at a source edge port, is filtered by that port's ACL, and
     * is forwarded hop by hop until it leaves at some edge port. Not one
     * decision-diagram operation happens in here: every question is a bitset
     * intersection, which is the whole point of the atomic-predicate stage.
     */
    public Reach reachability(Atoms ap) {
        int n = ap.size();
        Map<Port, BitSet> portAtoms = new HashMap<>();
        for (Map.Entry<Port, Integer> e : portPredicate.entrySet()) {
            portAtoms.put(e.getKey(), ap.setOf.getOrDefault(e.getValue(), new BitSet()));
        }
        Map<String, BitSet> aclAtoms = new HashMap<>();
        for (Map.Entry<String, Integer> e : aclPredicate.entrySet()) {
            aclAtoms.put(e.getKey(), ap.setOf.getOrDefault(e.getValue(), new BitSet()));
        }

        // Adjacency: where a packet leaving this port arrives.
        Map<Port, Port> peer = new HashMap<>();
        for (Link l : data.links) {
            peer.put(new Port(l.devA(), l.portA()), new Port(l.devB(), l.portB()));
            peer.put(new Port(l.devB(), l.portB()), new Port(l.devA(), l.portA()));
        }
        Map<String, List<Port>> devicePorts = new HashMap<>();
        for (Port p : portPredicate.keySet()) {
            devicePorts.computeIfAbsent(p.device(), x -> new ArrayList<>()).add(p);
        }

        int pairs = 0;
        for (Port src : data.edgePorts) {
            BitSet start = new BitSet(n);
            start.set(0, n);
            String acl = data.aclMap.get(src);
            if (acl != null && aclAtoms.containsKey(acl)) start.and(aclAtoms.get(acl));
            if (start.isEmpty()) continue;

            // Frontier keyed by device; a device is revisited only when new
            // atoms arrive, which is what bounds the walk.
            Map<String, BitSet> seen = new HashMap<>();
            Deque<String> queue = new ArrayDeque<>();
            seen.put(src.device(), (BitSet) start.clone());
            queue.add(src.device());

            java.util.Set<Port> reached = new java.util.HashSet<>();
            while (!queue.isEmpty()) {
                String dev = queue.poll();
                BitSet here = (BitSet) seen.get(dev).clone();
                for (Port out : devicePorts.getOrDefault(dev, List.of())) {
                    BitSet carried = (BitSet) here.clone();
                    carried.and(portAtoms.getOrDefault(out, new BitSet()));
                    if (carried.isEmpty()) continue;

                    Port next = peer.get(out);
                    if (next == null) {                 // host-facing port: arrival
                        if (!out.equals(src)) reached.add(out);
                        continue;
                    }
                    BitSet known = seen.get(next.device());
                    if (known == null) {
                        seen.put(next.device(), carried);
                        queue.add(next.device());
                    } else {
                        BitSet grown = (BitSet) known.clone();
                        grown.or(carried);
                        if (!grown.equals(known)) {
                            seen.put(next.device(), grown);
                            queue.add(next.device());
                        }
                    }
                }
            }
            pairs += reached.size();
        }
        return new Reach(pairs, n);
    }
}
