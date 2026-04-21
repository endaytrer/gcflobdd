// Smoke test for the boolean GCFLOBDD C ABI.
// Builds (x0 XOR x1 XOR x2 XOR x3) over a BDD(4) grammar and checks
// satisfiability + a simple de Morgan identity.

#include <cassert>
#include <cstdint>
#include <cstdio>

#include "gcflobdd.h"

int main() {
    gcfl_session_t* s = gcfl_session_new_bdd(4);
    if (!s) { std::fprintf(stderr, "session creation failed\n"); return 1; }

    gcfl_bool_t* x0 = gcfl_bool_mk_projection(s, 0);
    gcfl_bool_t* x1 = gcfl_bool_mk_projection(s, 1);
    gcfl_bool_t* x2 = gcfl_bool_mk_projection(s, 2);
    gcfl_bool_t* x3 = gcfl_bool_mk_projection(s, 3);

    gcfl_bool_t* a = gcfl_bool_xor(s, x0, x1);
    gcfl_bool_t* b = gcfl_bool_xor(s, a, x2);
    gcfl_bool_t* parity = gcfl_bool_xor(s, b, x3);

    // find a satisfying assignment of parity
    size_t len = 0;
    int8_t* assign = gcfl_bool_find_sat(parity, &len);
    assert(assign != nullptr && len == 4);
    int ones = 0;
    for (size_t i = 0; i < len; ++i) {
        std::printf("  x%zu = %d\n", i, assign[i]);
        if (assign[i] == 1) ++ones;
    }
    assert(ones % 2 == 1); // odd parity
    gcfl_assignment_free(assign, len);

    // de Morgan: NOT(a AND b) == (NOT a) OR (NOT b)
    gcfl_bool_t* and_ab = gcfl_bool_and(s, x0, x1);
    gcfl_bool_t* not_and = gcfl_bool_not(and_ab);
    gcfl_bool_t* not_a = gcfl_bool_not(x0);
    gcfl_bool_t* not_b = gcfl_bool_not(x1);
    gcfl_bool_t* or_nots = gcfl_bool_or(s, not_a, not_b);
    assert(gcfl_bool_eq(not_and, or_nots));

    std::printf("node_count (before gc) = %zu\n", gcfl_session_node_count(s));

    // tear down: free all handles before the session
    gcfl_bool_free(or_nots);
    gcfl_bool_free(not_b);
    gcfl_bool_free(not_a);
    gcfl_bool_free(not_and);
    gcfl_bool_free(and_ab);
    gcfl_bool_free(parity);
    gcfl_bool_free(b);
    gcfl_bool_free(a);
    gcfl_bool_free(x3);
    gcfl_bool_free(x2);
    gcfl_bool_free(x1);
    gcfl_bool_free(x0);

    gcfl_session_gc(s);
    std::printf("node_count (after gc)  = %zu\n", gcfl_session_node_count(s));

    gcfl_session_free(s);
    std::puts("smoke OK");
    return 0;
}
