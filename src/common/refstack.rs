use std::fmt::{Debug, Formatter};

#[derive(Copy, Clone)]
pub enum RefStack<'a, T> {
  Root(T),
  Extend(&'a RefStack<'a, T>, T),
}

impl<'a, T> RefStack<'a, T> {
  pub fn new(value: T) -> RefStack<'a, T> {
    RefStack::Root(value)
  }

  pub fn push(&self, value: T) -> RefStack<T> {
    RefStack::Extend(self, value)
  }

  pub fn len(&self) -> usize {
    match self {
      RefStack::Root(_) => 1,
      RefStack::Extend(parent, _) => parent.len() + 1,
    }
  }

  pub fn get(&self) -> &T {
    match self {
      RefStack::Root(value) => value,
      RefStack::Extend(_, value) => value,
    }
  }
}

impl<'a, T> Debug for RefStack<'a, T>
where
  T: Debug,
{
  fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
    match self {
      RefStack::Root(value) => f.write_fmt(format_args!("Stack: {:?}", value)),
      RefStack::Extend(parent, value) => {
        parent.fmt(f)?;
        f.write_fmt(format_args!(", {:?}", value))
      }
    }
  }
}

#[cfg(test)]
mod tests {
  use crate::common::refstack::RefStack;

  fn cascade<'a>(dist: usize, bc: &'a RefStack<'a, usize>) {
    let push = bc.push(6 - dist);
    assert_eq!(7 - dist, push.len());
    if dist > 0 {
      cascade(dist - 1, &push);
    }
  }

  #[test]
  fn test_refstack() {
    let a = RefStack::new(0);
    cascade(5, &a);
  }
}
