pub(super) fn inverse_lookup<T: Eq>(return_map: &[T], value: &T) -> Option<usize> {
    return_map.iter().position(|x| *x == *value)
}
pub(super) fn complement(return_map: &[bool]) -> Vec<bool> {
    return_map.iter().map(|x| !x).collect()
}
