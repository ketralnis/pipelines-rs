//! A tool for constructing multi-threaded pipelines of execution
//!
//! A `Pipeline` consists in one or more `PipelineEntry`s that each runs in its
//! own thread (or multiple threads in the case of `Multiplex`). They take in
//! items from the previous entry and produce items for the next entry, similar
//! to a Unix pipeline. This allows for expressing computation as a series of
//! steps that feed into each other and run concurrently
//!
//! # Examples
//!
//! Build the first 10 fibonacci numbers:
//!
//! ```rust
//! use pipelines::Pipeline;
//!
//! fn fibonacci(n:u64)->u64{if n<2 {1} else {fibonacci(n-1) + fibonacci(n-2)}}
//!
//! let nums: Vec<u64> = (0..10).collect();
//! let fibs: Vec<u64> = Pipeline::from(nums)
//!     .map(fibonacci)
//!     .into_iter().collect();
//! ```
//!
//! Build the first 10 fibonacci numbers in parallel, then double them:
//!
//! ```rust
//! use pipelines::{Pipeline, Mapper, Multiplex};
//!
//! let workers = 2;
//! fn fibonacci(n:u64)->u64{if n<2 {1} else {fibonacci(n-1) + fibonacci(n-2)}}
//!
//! let nums: Vec<u64> = (0..10).collect();
//! let fibs: Vec<u64> = Pipeline::from(nums)
//!     .then(Multiplex::from(Mapper::new(fibonacci), workers))
//!     .map(|x| x*2)
//!     .into_iter().collect();
//! ```

// HEADUPS: Keep that ^^ in sync with README.md

use std::sync::mpsc;
use std::thread;
use std::hash::Hash;
use std::collections::HashMap;

pub use filter::Filter;
pub use map::Mapper;
pub use multiplex::Multiplex;

#[derive(Debug)]
pub struct Sender<Out> {
    tx: mpsc::SyncSender<Out>,
    config: PipelineConfig,
}

impl<Out> Sender<Out> {
    /// Transmit a value to the next stage in the pipeline
    ///
    /// Panics on failure
    pub fn send(&self, out: Out) -> () {
        self.tx.send(out).expect("failed send");
    }

    fn pair(config: PipelineConfig) -> (Self, Receiver<Out>) {
        let (tx, rx) = mpsc::sync_channel(config.buff_size);
        (Self { tx, config }, Receiver { rx })
    }
}

impl<Out> Clone for Sender<Out> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            config: self.config.clone(),
        }
    }
}

#[derive(Debug)]
pub struct Receiver<In> {
    rx: mpsc::Receiver<In>,
}

impl<In> Receiver<In> {
    pub fn recv(&mut self) -> Option<In> {
        match self.rx.recv() {
            Ok(val) => Some(val),
            Err(_recv_err) => {
                // can only fail on hangup
                None
            }
        }
    }
}

impl<In> IntoIterator for Receiver<In> {
    type Item = In;
    type IntoIter = ReceiverIntoIterator<In>;

    fn into_iter(self) -> Self::IntoIter {
        ReceiverIntoIterator {
            iter: self.rx.into_iter(),
        }
    }
}

pub struct ReceiverIntoIterator<In> {
    iter: mpsc::IntoIter<In>,
}

impl<In> Iterator for ReceiverIntoIterator<In> {
    type Item = In;

    fn next(&mut self) -> Option<In> {
        self.iter.next()
    }
}

#[derive(Debug, Copy, Clone)]
pub struct PipelineConfig {
    buff_size: usize,
}

impl PipelineConfig {
    /// Set the size of the internal mpsc buffer.
    ///
    /// This can affect the effective parallelism and the length of the backlog between stages when
    /// different stages of the pipeline take different amounts of time
    pub fn buff_size(self, buff_size: usize) -> Self {
        Self { buff_size, ..self }
    }
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self { buff_size: 10 }
    }
}

#[derive(Debug)]
pub struct Pipeline<Output>
where
    Output: Send + 'static,
{
    rx: Receiver<Output>,
    config: PipelineConfig,
}

impl<Output> Pipeline<Output>
where
    Output: Send,
{
    /// Start a Pipeline
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::io::{self, BufRead};
    /// use pipelines::Pipeline;
    /// let pl = Pipeline::new(|tx| {
    ///     let stdin = io::stdin();
    ///     for line in stdin.lock().lines() {
    ///         tx.send(line.unwrap());
    ///     }
    /// });
    /// ```
    pub fn new<F>(func: F) -> Self
    where
        F: FnOnce(Sender<Output>) -> () + Send + 'static,
    {
        let config = PipelineConfig::default();
        let (tx, rx) = Sender::pair(config);
        thread::spawn(move || func(tx));
        Pipeline { rx, config }
    }

    /// Start a pipeline from an IntoIterator
    pub fn from<I>(source: I) -> Pipeline<Output>
    where
        I: IntoIterator<Item = Output> + Send + 'static,
    {
        Self::new(move |tx| {
            for item in source {
                tx.send(item);
            }
        })
    }

    /// Change the configuration of the pipeline
    ///
    /// Note that this applies to stages occurring *after* the config, not before.
    pub fn configure(self, config: PipelineConfig) -> Self {
        Pipeline {
            rx: self.rx,
            config,
        }
    }

    /// Given another `PipelineEntry` `next`, send the results of the previous
    /// entry into it
    ///
    /// # Examples
    ///
    /// ```rust
    /// use pipelines::{Pipeline, Multiplex, Mapper};
    ///
    /// let workers = 2;
    /// fn fibonacci(n:u64)->u64{if n<2 {1} else {fibonacci(n-1) + fibonacci(n-2)}}
    ///
    /// let nums: Vec<u64> = (0..10).collect();
    /// let fibs: Vec<u64> = Pipeline::from(nums)
    ///     .then(Multiplex::from(Mapper::new(fibonacci), workers))
    ///     .map(|x| x*2)
    ///     .into_iter().collect();
    /// ```
    pub fn then<EntryOut, Entry>(self, next: Entry) -> Pipeline<EntryOut>
    where
        Entry: PipelineEntry<Output, EntryOut> + Send + 'static,
        EntryOut: Send,
    {
        self.pipe(move |tx, rx| next.process(tx, rx))
    }

    /// Express a `PipelineEntry` as a closure
    ///
    /// # Example
    ///
    /// Take some directories and collect their contents
    ///
    /// ```rust
    /// use pipelines::Pipeline;
    /// use std::fs;
    /// use std::path::PathBuf;
    /// let directories = vec!["/usr/bin", "/usr/local/bin"];
    ///
    /// let found_files: Vec<PathBuf> = Pipeline::from(directories)
    ///     .pipe(|dirs, out| {
    ///         for dir in dirs {
    ///             for path in fs::read_dir(dir).unwrap() {
    ///                 out.send(path.unwrap().path());
    ///             }
    ///         }
    ///     })
    ///     .into_iter().collect();
    /// ```
    pub fn pipe<EntryOut, Func>(self, func: Func) -> Pipeline<EntryOut>
    where
        Func: FnOnce(Receiver<Output>, Sender<EntryOut>) -> (),
        Func: Send + 'static,
        EntryOut: Send,
    {
        let config = self.config.clone();
        let (tx, rx) = Sender::pair(config.clone());
        thread::spawn(move || {
            func(self.rx, tx);
        });

        Pipeline { rx, config: config }
    }

    /// Call `func` on every entry in the pipeline
    ///
    /// # Example
    ///
    /// Double every number
    ///
    /// ```rust
    /// use pipelines::Pipeline;
    /// let nums: Vec<u64> = (0..10).collect();
    ///
    /// let doubled: Vec<u64> = Pipeline::from(nums)
    ///     .map(|x| x*2)
    ///     .into_iter().collect();
    /// ```
    pub fn map<EntryOut, Func>(self, func: Func) -> Pipeline<EntryOut>
    where
        Func: Fn(Output) -> EntryOut + Send + 'static,
        EntryOut: Send,
    {
        self.then(map::Mapper::new(func))
    }

    /// Pass items into the next `PipelineEntry` only if `pred` is true
    ///
    /// # Example
    ///
    /// Pass on only even numbers
    ///
    /// ```rust
    /// use pipelines::Pipeline;
    /// let nums: Vec<u64> = (0..10).collect();
    ///
    /// let evens: Vec<u64> = Pipeline::from(nums)
    ///     .filter(|x| x%2 == 0)
    ///     .into_iter().collect();
    /// ```
    pub fn filter<Func>(self, pred: Func) -> Pipeline<Output>
    where
        Func: Fn(&Output) -> bool + Send + 'static,
    {
        self.then(filter::Filter::new(pred))
    }

    /// Consume this Pipeline without collecting the results
    ///
    /// Can be useful if the work was done in the last `PipelineEntry`
    ///
    /// # Example
    ///
    /// ```rust
    /// use pipelines::Pipeline;
    /// let nums: Vec<u64> = (0..10).collect();
    ///
    /// Pipeline::from(nums)
    ///     .map(|fname| /* something with side-effects */ ())
    ///     .drain(); // no results to pass on
    /// ```
    pub fn drain(self) {
        for _ in self {}
    }
}

impl<OutKey, OutValue> Pipeline<(OutKey, OutValue)>
where
    OutKey: Hash + Eq + Send,
    OutValue: Send,
{
    /// The reduce phase of a mapreduce-type pipeline.
    ///
    /// The previous entry must have sent tuples of (Key, Value), and this entry
    /// groups them by Key and calls func once per Key
    ///
    /// # Example
    ///
    ///
    /// ```rust
    /// use pipelines::Pipeline;
    /// let nums: Vec<u64> = (0..10).collect();
    ///
    /// // find the sum of the even/odd numbers in the doubles of 0..10
    /// let biggests: Vec<(bool, u64)> = Pipeline::from(nums)
    ///     .map(|x| (x % 2 == 0, x*2))
    ///     .reduce(|evenness, nums| (evenness, *nums.iter().max().unwrap()))
    ///     .into_iter().collect();
    /// ```
    pub fn reduce<EntryOut, Func>(self, func: Func) -> Pipeline<EntryOut>
    where
        Func: Fn(OutKey, Vec<OutValue>) -> EntryOut + Send + 'static,
        EntryOut: Send,
    {
        self.pipe(move |rx, tx| {
            // gather up all of the values and group them by key
            let mut by_key: HashMap<OutKey, Vec<OutValue>> = HashMap::new();
            for inbound in rx {
                let (key, value) = inbound;
                by_key.entry(key).or_insert_with(Vec::new).push(value)
            }

            // now that we have them all grouped by key, we can run the reducer on the groups
            for (key, values) in by_key.into_iter() {
                let output = func(key, values);
                tx.send(output);
            }
        })
    }
}

impl<Output> IntoIterator for Pipeline<Output>
where
    Output: Send,
{
    type Item = Output;
    type IntoIter = ReceiverIntoIterator<Output>;

    fn into_iter(self) -> ReceiverIntoIterator<Output> {
        self.rx.into_iter()
    }
}

/// The trait that entries in the pipeline must implement
pub trait PipelineEntry<In, Out> {
    fn process<I: IntoIterator<Item = In>>(self, rx: I, tx: Sender<Out>) -> ();
}

mod map {
    use std::marker::PhantomData;

    use super::{PipelineEntry, Sender};

    /// A pipeline entry representing a function to be run on each value and its
    /// result to be sent down the pipeline
    #[derive(Debug)]
    pub struct Mapper<In, Out, Func>
    where
        Func: Fn(In) -> Out,
    {
        func: Func,

        // make the compiler happy
        in_: PhantomData<In>,
        out_: PhantomData<Out>,
    }

    /// Make a new `Mapper` out of a function
    impl<In, Out, Func> Mapper<In, Out, Func>
    where
        Func: Fn(In) -> Out,
    {
        pub fn new(func: Func) -> Self {
            Mapper {
                func,
                in_: PhantomData,
                out_: PhantomData,
            }
        }
    }

    impl<In, Out, Func> PipelineEntry<In, Out> for Mapper<In, Out, Func>
    where
        Func: Fn(In) -> Out,
    {
        fn process<I: IntoIterator<Item = In>>(self, rx: I, tx: Sender<Out>) {
            for item in rx {
                let mapped = (self.func)(item);
                tx.send(mapped);
            }
        }
    }

    impl<In, Out, Func> Clone for Mapper<In, Out, Func>
    where
        Func: Fn(In) -> Out + Copy,
    {
        fn clone(&self) -> Self {
            Mapper::new(self.func)
        }
    }

    impl<In, Out, Func> Copy for Mapper<In, Out, Func>
    where
        Func: Fn(In) -> Out + Copy,
    {
    }
}

mod filter {
    use std::marker::PhantomData;

    use super::{PipelineEntry, Sender};

    /// A pipeline entry with a predicate that values must beet to be sent
    /// further in the pipeline
    #[derive(Debug)]
    pub struct Filter<In, Func>
    where
        Func: Fn(&In) -> bool,
    {
        func: Func,

        // make the compiler happy
        in_: PhantomData<In>,
    }

    /// Make a new `Filter` out of a predicate function
    impl<In, Func> Filter<In, Func>
    where
        Func: Fn(&In) -> bool,
    {
        pub fn new(func: Func) -> Self {
            Filter {
                func,
                in_: PhantomData,
            }
        }
    }

    impl<In, Func> PipelineEntry<In, In> for Filter<In, Func>
    where
        Func: Fn(&In) -> bool,
    {
        fn process<I: IntoIterator<Item = In>>(self, rx: I, tx: Sender<In>) {
            for item in rx {
                if (self.func)(&item) {
                    tx.send(item);
                }
            }
        }
    }
}

mod multiplex {
    // work around https://github.com/rust-lang/rust/issues/28229
    // (functions implement Copy but not Clone)
    #![cfg_attr(feature = "cargo-clippy", allow(expl_impl_clone_on_copy))]

    use std::marker::PhantomData;
    use std::sync::{Arc, Mutex};
    use std::thread;

    use super::{PipelineConfig, PipelineEntry, Receiver, Sender};

    /// A meta pipeline entry that distributes the work of a `PipelineEntry`
    /// across multiple threads
    #[derive(Debug)]
    pub struct Multiplex<In, Out, Entry>
    where
        Entry: PipelineEntry<In, Out> + Send,
    {
        entries: Vec<Entry>,

        // make the compiler happy
        in_: PhantomData<In>,
        out_: PhantomData<Out>,
    }

    /// Build a `Multiplex` by copying an existing `PipelineEntry`
    ///
    /// Note: this is only applicable where the `PipelineEntry` implements Copy,
    /// which due to [Rust #28229](https://github.com/rust-lang/rust/issues/28229)
    /// is not true of closures
    impl<In, Out, Entry> Multiplex<In, Out, Entry>
    where
        Entry: PipelineEntry<In, Out> + Send + Copy,
    {
        pub fn from(entry: Entry, workers: usize) -> Self {
            Self::new((0..workers).map(|_| entry).collect())
        }
    }

    impl<In, Out, Entry> Multiplex<In, Out, Entry>
    where
        Entry: PipelineEntry<In, Out> + Send,
    {
        pub fn new(entries: Vec<Entry>) -> Self {
            Multiplex {
                entries,
                in_: PhantomData,
                out_: PhantomData,
            }
        }
    }

    #[cfg(feature = "chan")]
    extern crate chan;

    impl<In, Out, Entry> PipelineEntry<In, Out> for Multiplex<In, Out, Entry>
    where
        Entry: PipelineEntry<In, Out> + Send + 'static,
        In: Send + 'static,
        Out: Send + 'static,
    {
        fn process<I: IntoIterator<Item = In>>(
            mut self,
            rx: I,
            tx: Sender<Out>,
        ) {
            if self.entries.len() == 1 {
                // if there's only one entry we can skip most of the work.
                // this way client libraries can just create multiplexers
                // without having to worry about wasting performance in the
                // simple case
                let entry = self.entries.pop().expect("len 1 but no entries?");
                return entry.process(rx, tx);
            }

            // TODO both of these methods use PipelineConfig::default() to size their internal
            // channel buffers are aren't able to customise them

            if cfg!(feature = "chan") {
                // if we're compiled when `chan` support, use that
                let (chan_tx, chan_rx) =
                    chan::sync(PipelineConfig::default().buff_size);

                for entry in self.entries {
                    let entry_rx = chan_rx.clone();
                    let entry_tx = tx.clone();

                    thread::spawn(move || {
                        entry.process(entry_rx, entry_tx);
                    });
                }

                for item in rx {
                    chan_tx.send(item);
                }
            } else {
                // if we weren't compiled with `chan` use a Mutex<rx>. workers
                // will read their work out of this channel but send their
                // results directly into the regular tx channel

                let (master_tx, chan_rx) =
                    Sender::pair(PipelineConfig::default());
                let chan_rx = LockedRx::new(chan_rx);

                for entry in self.entries {
                    let entry_rx = chan_rx.clone();
                    let entry_tx = tx.clone();

                    thread::spawn(move || {
                        entry.process(entry_rx, entry_tx);
                    });
                }

                // now we copy the work from rx into the shared channel. the
                // workers will be putting their results into tx directly so
                // this is the only shuffling around that we have to do
                for item in rx {
                    master_tx.send(item);
                }
            }
        }
    }

    struct LockedRx<T>
    where
        T: Send,
    {
        lockbox: Arc<Mutex<Receiver<T>>>,
    }

    impl<T> LockedRx<T>
    where
        T: Send,
    {
        pub fn new(recv: Receiver<T>) -> Self {
            Self {
                lockbox: Arc::new(Mutex::new(recv)),
            }
        }
    }

    impl<T> Clone for LockedRx<T>
    where
        T: Send,
    {
        fn clone(&self) -> Self {
            Self {
                lockbox: self.lockbox.clone(),
            }
        }
    }

    impl<T> Iterator for LockedRx<T>
    where
        T: Send,
    {
        type Item = T;

        fn next(&mut self) -> Option<T> {
            self.lockbox.lock().expect("failed unwrap mutex").recv()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple() {
        let source: Vec<i32> = vec![1, 2, 3];
        let pbb: Pipeline<i32> = Pipeline::from(source);
        let produced: Vec<i32> = pbb.into_iter().collect();

        assert_eq!(produced, vec![1, 2, 3]);
    }

    #[test]
    fn map() {
        let source: Vec<i32> = (1..1000).collect();
        let expect: Vec<i32> = source.iter().map(|x| x * 2).collect();

        let pbb: Pipeline<i32> = Pipeline::from(source).map(|i| i * 2);
        let produced: Vec<i32> = pbb.into_iter().collect();

        assert_eq!(produced, expect);
    }

    #[test]
    fn multiple_map() {
        let source: Vec<i32> = vec![1, 2, 3];
        let expect: Vec<i32> =
            source.iter().map(|x| (x * 2) * (x * 2)).collect();

        let pbb: Pipeline<i32> =
            Pipeline::from(source).map(|i| i * 2).map(|i| i * i);
        let produced: Vec<i32> = pbb.into_iter().collect();

        assert_eq!(produced, expect);
    }

    // just something expensive
    fn fib_work(n: u64) -> u64 {
        const WORK_FACTOR: u64 = 10;
        fib(WORK_FACTOR) + n
    }

    fn fib(n: u64) -> u64 {
        if n == 0 || n == 1 {
            1
        } else {
            fib(n - 1) + fib(n - 2)
        }
    }

    #[test]
    fn multiplex_map_function() {
        // we have two signatures for Multiplex, one that takes a function
        // pointer and one that can take a closure. This is the function pointer
        // side

        let workers: usize = 10;

        let source: Vec<u64> = (1..1000).collect();
        let expect: Vec<u64> =
            source.clone().into_iter().map(fib_work).collect();

        let pbb: Pipeline<u64> = Pipeline::from(source).then(
            multiplex::Multiplex::from(map::Mapper::new(fib_work), workers),
        );
        let mut produced: Vec<u64> = pbb.into_iter().collect();

        produced.sort(); // these may arrive out of order
        assert_eq!(produced, expect);
    }

    #[test]
    fn multiplex_map_closure() {
        let workers: usize = 10;

        let source: Vec<i32> = (1..1000).collect();
        let expect: Vec<i32> = source.iter().map(|x| x * 2).collect();

        let pbb: Pipeline<i32> =
            Pipeline::from(source).then(multiplex::Multiplex::new(
                (0..workers).map(|_| map::Mapper::new(|i| i * 2)).collect(),
            ));
        let mut produced: Vec<i32> = pbb.into_iter().collect();

        produced.sort(); // these may arrive out of order
        assert_eq!(produced, expect);
    }

    #[test]
    fn filter() {
        let source: Vec<i32> = (1..1000).collect();
        let expect: Vec<i32> = source
            .iter()
            .map(|x| x + 1)
            .filter(|x| x % 2 == 0)
            .collect();

        let pbb: Pipeline<i32> =
            Pipeline::from(source).map(|i| i + 1).filter(|i| i % 2 == 0);
        let produced: Vec<i32> = pbb.into_iter().collect();

        assert_eq!(produced, expect);
    }

    #[test]
    fn simple_closure() {
        let source: Vec<i32> = (1..1000).collect();
        let expect: Vec<i32> = source
            .iter()
            .map(|x| x + 1)
            .filter(|x| x % 2 == 0)
            .collect();

        let pbb: Pipeline<i32> = Pipeline::from(source).pipe(|in_, out| {
            for item in in_ {
                let item = item + 1;
                if item % 2 == 0 {
                    out.send(item);
                }
            }
        });
        let produced: Vec<i32> = pbb.into_iter().collect();

        assert_eq!(produced, expect);
    }
}
