use hashbrown::HashSet;
use std::collections::VecDeque;
use std::hash::Hash;

pub struct QueueSet<T> {
    queue: VecDeque<T>,
    set: HashSet<T>,
}

impl<T> QueueSet<T>
where
    T: Clone + Eq + Hash,
{
    pub fn new() -> Self {
        Self {
            queue: VecDeque::new(),
            set: HashSet::new(),
        }
    }

    pub fn push_back(&mut self, e: T) {
        if self.set.get(&e).is_some() {
        } else {
            self.set.insert(e.clone());
            self.queue.push_back(e);
        }
    }

    pub fn pop_front(&mut self) -> Option<T> {
        if let Some(x) = self.queue.pop_front() {
            self.set.remove(&x);
            Some(x)
        } else {
            None
        }
    }
}
