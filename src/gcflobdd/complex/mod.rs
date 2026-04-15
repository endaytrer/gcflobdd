use rug::Complex;

use crate::gcflobdd::GcflobddT;

pub type GcflobddComplex<'grammar> = GcflobddT<'grammar, Complex>;
