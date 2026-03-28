pub(super) type ReturnMapT<T> = Vec<T>;
pub(super) type ReturnMap = ReturnMapT<usize>;

pub(super) fn inverse_lookup<T: Eq>(return_map: &ReturnMapT<T>, value: &T) -> Option<usize> {
    return_map.iter().position(|x| *x == *value)
}
pub(super) fn complement(return_map: &ReturnMapT<bool>) -> ReturnMapT<bool> {
    return_map.iter().map(|x| !x).collect()
}
