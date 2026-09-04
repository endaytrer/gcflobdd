package netbench;

/**
 * NDD's 104-bit TCP/IP five-tuple layout, which every backend here must
 * reproduce exactly.
 *
 * <p>Field order and widths come from
 * {@code application/wan/ndd/verifier/common/BDDACLWrapper.java:73-115}:
 * src_ip(32), dst_ip(32), src_port(16), dst_port(16), protocol(8), declared in
 * that order, so field index == NDD level == flat variable block.
 *
 * <p><b>Bit 0 of a field is its MSB</b> ({@code NDD.restrict} javadoc;
 * {@code NDD.encodeBinaryPrefixLabel} walks {@code prefixBinary[i]} against
 * {@code bddVars[i]} ascending). A prefix of length {@code p} therefore
 * constrains bits {@code 0..p-1}.
 */
public final class Fields {
    private Fields() {}

    public static final int SRC_IP = 0;
    public static final int DST_IP = 1;
    public static final int SRC_PORT = 2;
    public static final int DST_PORT = 3;
    public static final int PROTOCOL = 4;

    public static final int COUNT = 5;
    public static final int[] WIDTH = {32, 32, 16, 16, 8};
    public static final String[] NAME = {"src_ip", "dst_ip", "src_port", "dst_port", "protocol"};

    /** 104. */
    public static final int TOTAL_BITS = 32 + 32 + 16 + 16 + 8;

    /** Flat variable index of bit {@code bit} (0 = MSB) of {@code field}. */
    public static int flat(int field, int bit) {
        int base = 0;
        for (int f = 0; f < field; f++) base += WIDTH[f];
        return base + bit;
    }
}
