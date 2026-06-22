use gcflobdd::declare_grammar;
use gcflobdd::grammar::GhddGrammar;
use gcflobdd::grammar::Unit;

// Grammars may reference one another; each level's NUM_VARS is the sum over its
// components, and shared component suffixes share generated wrapper types.
declare_grammar! {
    G5 => G4, G4;
    G4 => G3, G3;
    G3 => G2, G2;
    G2 => G1, G1, G1;
    G1 => Unit, Unit;
}

#[test]
fn nested_grammars() {
    assert_eq!(G1::NUM_VARS, 2);
    assert_eq!(G2::NUM_VARS, 6);
    assert_eq!(G3::NUM_VARS, 12);
    assert_eq!(G4::NUM_VARS, 24);
    assert_eq!(G5::NUM_VARS, 48);

    // Construction goes through the generated, interned node types.
    let _g5 = G5::mk_distinction(0);
}
