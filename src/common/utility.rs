// https://stackoverflow.com/questions/51272571/how-do-i-check-if-a-slice-is-sorted
pub fn is_sorted<T>(data: &[T]) -> bool
where
  T: Ord,
{
  data.windows(2).all(|w| w[0] <= w[1])
}
