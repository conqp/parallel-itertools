use std::fmt::Debug;
use std::sync::mpsc::{channel, Receiver};
use threadpool::{Builder, ThreadPool};

pub trait ThreadedIter<F, O>
where
    Self: Iterator + Sized,
    F: Fn(<Self as Iterator>::Item) -> O + Send + Clone + 'static,
    <Self as Iterator>::Item: Send + 'static,
    O: Send + Sync + Debug + 'static,
{
    /// Maps items of an iterator in parallel while conserving their order
    /// # Examples
    /// ```
    /// use parallel_itertools::ThreadedIter;
    /// let items = vec![1, 2, 3, 4, 5, 6];
    /// let target: Vec<_> = items.iter().map(i32::to_string).collect();
    ///
    /// let result: Vec<_> = items
    ///     .into_iter()
    ///     .parallel_map(|item| item.to_string(), None)
    ///     .collect();
    ///
    /// assert_eq!(result, target);
    /// ```
    fn parallel_map(self, f: F, num_threads: Option<usize>) -> ThreadedMap<Self, F, O> {
        ThreadedMap::new(self, f, num_threads)
    }
}

pub struct ThreadedMap<I, F, O>
where
    I: Iterator,
    F: Fn(<I as Iterator>::Item) -> O + Send + Clone + 'static,
    <I as Iterator>::Item: Send + 'static,
    O: Send + Sync + Debug + 'static,
{
    iterator: I,
    function: F,
    thread_pool: ThreadPool,
    window: Vec<O>,
}

impl<I, F, O> ThreadedMap<I, F, O>
where
    I: Iterator,
    F: Fn(<I as Iterator>::Item) -> O + Send + Clone + 'static,
    <I as Iterator>::Item: Send + 'static,
    O: Send + Sync + Debug + 'static,
{
    pub fn new(iterator: I, function: F, num_threads: Option<usize>) -> Self {
        Self {
            iterator,
            function,
            thread_pool: num_threads.map_or_else(|| Builder::new().build(), ThreadPool::new),
            window: Vec::new(),
        }
    }

    fn window(&mut self, n: usize) -> Vec<<I as Iterator>::Item> {
        let mut items = Vec::new();

        for _ in 0..n {
            if let Some(item) = self.iterator.next() {
                items.push(item);
            }
        }

        items
    }

    fn send_items(&mut self) -> Receiver<(usize, O)> {
        let (tx, rx) = channel::<(usize, O)>();
        let window = self.window(self.thread_pool.max_count());

        for (index, item) in window.into_iter().enumerate() {
            let tx = tx.clone();
            let f = self.function.clone();
            self.thread_pool.execute(move || {
                tx.send((index, (f)(item)))
                    .expect("channel will be there waiting for the pool");
            });
        }

        rx
    }
}

impl<I, F, O> Iterator for ThreadedMap<I, F, O>
where
    I: Iterator,
    F: Fn(<I as Iterator>::Item) -> O + Send + Clone + 'static,
    <I as Iterator>::Item: Send + 'static,
    O: Send + Sync + Debug + 'static,
{
    type Item = O;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(item) = self.window.pop() {
            return Some(item);
        }

        let rx = self.send_items();
        let mut window: Vec<_> = rx.iter().collect();

        if window.is_empty() {
            return None;
        }

        window.sort_by(|(lhs, _), (rhs, _)| rhs.cmp(lhs));
        self.window = window.into_iter().map(|(_, item)| item).collect();
        self.window.pop()
    }
}

impl<I, F, O> ThreadedIter<F, O> for I
where
    I: Iterator,
    F: Fn(<I as Iterator>::Item) -> O + Send + Clone + 'static,
    <I as Iterator>::Item: Send + 'static,
    O: Send + Sync + Debug + 'static,
{
}
