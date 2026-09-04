pub mod gcflobdd;
pub mod grammar;
#[cfg(feature = "sync")]
pub mod sync;

/// Internals exposed only so the profiling harnesses in `tests/` can reach
/// [`utils::opcount`]. Not part of the API.
#[doc(hidden)]
pub mod utils;
