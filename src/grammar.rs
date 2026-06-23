use std::fmt::Debug;

use crate::gcflobdd::{
    connection::Connection,
    grouping::{BddGrouping, Grouping, UnitGrouping},
};
use crate::utils::hash_cache::Rch;
pub trait GhddGrammar {
    const NUM_VARS: usize;
    type Grouping: Grouping<Grammar = Self>;
    fn mk_distinction(x: usize) -> Rch<Self::Grouping> {
        debug_assert!(x < Self::NUM_VARS, "variable index out of range");
        Self::Grouping::mk_distinction(x)
    }
    fn mk_no_distinction() -> Rch<Self::Grouping> {
        Self::Grouping::mk_no_distinction()
    }
}

#[derive(Debug, Clone)]
pub struct Unit;

impl GhddGrammar for Unit {
    const NUM_VARS: usize = 1;
    type Grouping = UnitGrouping;
}

pub struct BddGrammar<const N: usize>;

impl<const N: usize> GhddGrammar for BddGrammar<N> {
    const NUM_VARS: usize = N;
    type Grouping = BddGrouping<N>;
}

pub trait RecursiveGrammar: GhddGrammar {
    type Connection: Connection;
}
