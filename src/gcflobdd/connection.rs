use std::rc::Rc;

use crate::gcflobdd::{node::GcflobddNode, return_map::ReturnMap};

#[derive(Clone, Hash)]
pub(super) struct ConnectionT<'grammar, Handle> {
    pub entry_point: Rc<GcflobddNode<'grammar>>,
    pub return_map: Handle,
}
impl<'grammar, Handle: std::fmt::Debug> std::fmt::Debug for ConnectionT<'grammar, Handle> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectionT")
            .field("entry_point", &Rc::as_ptr(&self.entry_point))
            .field("return_map", &self.return_map)
            .finish()
    }
}

pub(crate) type Connection<'grammar> = ConnectionT<'grammar, ReturnMap>;

impl<'grammar> Connection<'grammar> {
    pub fn new(entry_point: Rc<GcflobddNode<'grammar>>) -> Self {
        Self {
            return_map: ReturnMap::new_sequential(entry_point.get_num_exits()),
            entry_point,
        }
    }
}
