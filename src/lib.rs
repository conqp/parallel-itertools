use std::sync::mpsc::{channel, Receiver};
use threadpool::{Builder, ThreadPool};

pub trait ThreadedMappable<F>
where
    Self: Iterator,
    F: FnOnce(<Self as Iterator>::Item) -> <Self::Iter as Iterator>::Item + Send + Clone,
    <Self as Iterator>::Item: Send,
    Self::Iter: Iterator,
    <Self::Iter as Iterator>::Item: Send + Sync,
{
    type Iter;

    /// Maps items of an iterator in parallel while conserving their order
    /// # Examples
    /// ```
    /// use threaded_map::ThreadedMappable;
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
    fn parallel_map(self, f: F, num_threads: Option<usize>) -> Self::Iter;
}

#[derive(Debug)]
pub struct ThreadedMap<I, F, O>
where
    I: Iterator,
    F: FnOnce(<I as Iterator>::Item) -> O + 'static,
    <I as Iterator>::Item: 'static,
    O: Send + 'static,
{
    iterator: I,
    function: F,
    thread_pool: ThreadPool,
    window: Vec<O>,
}

impl<I, F, O> ThreadedMap<I, F, O>
where
    I: Iterator,
    F: FnOnce(<I as Iterator>::Item) -> O + Send + Clone,
    <I as Iterator>::Item: Send,
    O: Send + Sync,
{
    pub fn new(iterator: I, function: F, num_threads: Option<usize>) -> Self {
        Self {
            iterator,
            function,
            thread_pool: num_threads.map_or_else(|| Builder::new().build(), ThreadPool::new),
            window: Vec::new(),
        }
    }

    fn send_items(&mut self) -> Receiver<(usize, O)> {
        let (tx, rx) = channel::<(usize, O)>();

        for (index, item) in (0..self.thread_pool.max_count())
            .map_while(|_| self.iterator.next())
            .enumerate()
        {
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
    F: FnOnce(<I as Iterator>::Item) -> O + Send + Clone,
    <I as Iterator>::Item: Send,
    O: Send + Sync,
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

impl<I, F, O> ThreadedMappable<F> for I
where
    I: Iterator,
    F: FnOnce(<I as Iterator>::Item) -> O + Clone + Send + 'static,
    <I as Iterator>::Item: Send + 'static,
    O: Send + Sync + 'static,
{
    type Iter = ThreadedMap<Self, F, O>;

    fn parallel_map(self, f: F, num_threads: Option<usize>) -> Self::Iter {
        ThreadedMap::new(self, f, num_threads)
    }
}
