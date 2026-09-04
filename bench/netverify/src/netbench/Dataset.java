package netbench;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** The five text files a generated fat-tree data plane consists of. */
public final class Dataset {

    /** One longest-prefix-match FIB entry. */
    public record FibEntry(String device, int port, long ip, int len) {}

    /** One end of an L1 link. */
    public record Link(String devA, int portA, String devB, int portB) {}

    /** A device/port pair. */
    public record Port(String device, int port) {}

    public final String name;
    public final List<Link> links = new ArrayList<>();
    public final List<FibEntry> fib = new ArrayList<>();
    public final List<Port> edgePorts = new ArrayList<>();
    public final Map<String, List<Rule>> acls = new LinkedHashMap<>();
    public final Map<Port, String> aclMap = new LinkedHashMap<>();

    private Dataset(String name) { this.name = name; }

    public static Dataset load(Path dir) throws IOException {
        Dataset d = new Dataset(dir.getFileName().toString());
        for (String l : lines(dir.resolve("topo.txt"))) {
            String[] t = l.split("\\s+");
            d.links.add(new Link(t[0], Integer.parseInt(t[1]), t[2], Integer.parseInt(t[3])));
        }
        for (String l : lines(dir.resolve("fib.txt"))) {
            String[] t = l.split("\\s+");
            long[] c = Rule.parseCidr(t[2]);
            d.fib.add(new FibEntry(t[0], Integer.parseInt(t[1]), c[0], (int) c[1]));
        }
        for (String l : lines(dir.resolve("edgeports.txt"))) {
            String[] t = l.split("\\s+");
            d.edgePorts.add(new Port(t[0], Integer.parseInt(t[1])));
        }
        for (String l : lines(dir.resolve("acl.txt"))) {
            int sp = l.indexOf(' ');
            d.acls.computeIfAbsent(l.substring(0, sp), x -> new ArrayList<>())
                  .add(Rule.parse(l.substring(sp + 1)));
        }
        for (String l : lines(dir.resolve("aclmap.txt"))) {
            String[] t = l.split("\\s+");
            d.aclMap.put(new Port(t[0], Integer.parseInt(t[1])), t[2]);
        }
        return d;
    }

    private static List<String> lines(Path p) throws IOException {
        List<String> out = new ArrayList<>();
        for (String l : Files.readAllLines(p)) {
            String s = l.trim();
            if (!s.isEmpty() && !s.startsWith("#")) out.add(s);
        }
        return out;
    }

    public int aclRuleCount() {
        return acls.values().stream().mapToInt(List::size).sum();
    }

    /** Distinct devices named by the topology -- the fat-tree's switch count. */
    public int deviceCount() {
        java.util.Set<String> devices = new java.util.HashSet<>();
        for (Link l : links) { devices.add(l.devA()); devices.add(l.devB()); }
        for (FibEntry e : fib) devices.add(e.device());
        return devices.size();
    }
}
