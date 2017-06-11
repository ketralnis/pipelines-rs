use std::sync::mpsc;
use std::thread;


#[derive(Debug)]
pub struct Pipeline<Output>
    where Output: Send + 'static
{
    rx: mpsc::Receiver<Output>,
}


impl<Output> Pipeline<Output>
    where Output: Send
{
    // start up the producer thread and start sending items into rx
    #[must_use]
    pub fn new<I>(source: I, buffsize: usize) -> Pipeline<Output>
        where I: IntoIterator<Item = Output> + Send + 'static
    {
        let (tx, rx) = mpsc::sync_channel(buffsize);
        thread::spawn(move || for item in source {
                          tx.send(item).expect("failed send (super)");
                      });

        Pipeline { rx }
    }

    // given another pipeline entry, send the results of the previous entry into
    // the next one
    #[must_use]
    pub fn then<EntryOut, Entry>(self,
                                 next: Entry,
                                 buffsize: usize)
                                 -> Pipeline<EntryOut>
        where Entry: PipelineEntry<Output, EntryOut> + Send + 'static,
              EntryOut: Send
    {
        self.pipe(move |tx, rx| next.process(tx, rx), buffsize)
    }

    pub fn pipe<EntryOut, Func>(self,
                                func: Func,
                                buffsize: usize)
                                -> Pipeline<EntryOut>
        where Func: FnOnce(mpsc::Receiver<Output>, mpsc::SyncSender<EntryOut>)
                           -> (),
              Func: Send + 'static,
              EntryOut: Send
    {
        let (tx, rx) = mpsc::sync_channel(buffsize);
        thread::spawn(move || { func(self.rx, tx); });

        Pipeline { rx }
    }

    pub fn map<EntryOut, Func>(self,
                               func: Func,
                               buffsize: usize)
                               -> Pipeline<EntryOut>
        where Func: Fn(Output) -> EntryOut + Send + 'static,
              EntryOut: Send
    {
        self.then(map::Mapper::new(func), buffsize)
    }

    pub fn filter<Func>(self, func: Func, buffsize: usize) -> Pipeline<Output>
        where Func: Fn(&Output) -> bool + Send + 'static
    {
        self.then(filter::Filter::new(func), buffsize)
    }

    pub fn drain(self) {
        for _ in self {}
    }
}


impl<Output> IntoIterator for Pipeline<Output>
    where Output: Send
{
    type Item = Output;
    type IntoIter = mpsc::IntoIter<Output>;

    #[must_use]
    fn into_iter(self) -> mpsc::IntoIter<Output> {
        self.rx.into_iter()
    }
}


pub trait PipelineEntry<In, Out> {
    fn process<I: IntoIterator<Item = In>>(self,
                                           rx: I,
                                           tx: mpsc::SyncSender<Out>)
                                           -> ();
}


pub mod map {
    use std::marker::PhantomData;
    use std::sync::mpsc;

    use super::PipelineEntry;

    #[derive(Debug)]
    pub struct Mapper<In, Out, Func>
        where Func: Fn(In) -> Out
    {
        func: Func,

        // make the compiler happy
        in_: PhantomData<In>,
        out_: PhantomData<Out>,
    }

    impl<In, Out, Func> Mapper<In, Out, Func>
        where Func: Fn(In) -> Out
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
        where Func: Fn(In) -> Out
    {
        fn process<I: IntoIterator<Item = In>>(self,
                                               rx: I,
                                               tx: mpsc::SyncSender<Out>) {
            for item in rx {
                let mapped = (self.func)(item);
                tx.send(mapped).expect("failed to send");
            }
        }
    }

    impl<In, Out, Func> Clone for Mapper<In, Out, Func>
        where Func: Fn(In) -> Out + Copy
    {
        fn clone(&self) -> Self {
            Mapper::new(self.func)
        }
    }

    impl<In, Out, Func> Copy for Mapper<In, Out, Func>
        where Func: Fn(In) -> Out + Copy
    {
    }
}


pub mod filter {
    use std::marker::PhantomData;
    use std::sync::mpsc;

    use super::PipelineEntry;

    #[derive(Debug)]
    pub struct Filter<In, Func>
        where Func: Fn(&In) -> bool
    {
        func: Func,

        // make the compiler happy
        in_: PhantomData<In>,
    }

    impl<In, Func> Filter<In, Func>
        where Func: Fn(&In) -> bool
    {
        pub fn new(func: Func) -> Self {
            Filter {
                func,
                in_: PhantomData,
            }
        }
    }

    impl<In, Func> PipelineEntry<In, In> for Filter<In, Func>
        where Func: Fn(&In) -> bool
    {
        fn process<I: IntoIterator<Item = In>>(self,
                                               rx: I,
                                               tx: mpsc::SyncSender<In>) {
            for item in rx {
                if (self.func)(&item) {
                    tx.send(item).expect("failed to send")
                }
            }
        }
    }
}


pub mod multiplex {
    // work around https://github.com/rust-lang/rust/issues/28229
    // (functions implement Copy but not Clone)
    #![cfg_attr(feature="cargo-clippy", allow(expl_impl_clone_on_copy))]

    use std::marker::PhantomData;
    use std::sync::mpsc;
    use std::sync::{Arc, Mutex};
    use std::thread;

    use super::PipelineEntry;

    #[derive(Debug)]
    pub struct Multiplex<In, Out, Entry>
        where Entry: PipelineEntry<In, Out> + Send
    {
        entries: Vec<Entry>,
        buffsize: usize,

        // make the compiler happy
        in_: PhantomData<In>,
        out_: PhantomData<Out>,
    }

    impl<In, Out, Entry> Multiplex<In, Out, Entry>
        where Entry: PipelineEntry<In, Out> + Send + Copy
    {
        pub fn from(entry: Entry, workers: usize, buffsize: usize) -> Self {
            Self::new((0..workers).map(|_| entry).collect(), buffsize)
        }
    }

    impl<In, Out, Entry> Multiplex<In, Out, Entry>
        where Entry: PipelineEntry<In, Out> + Send
    {
        pub fn new(entries: Vec<Entry>, buffsize: usize) -> Self {
            Multiplex {
                entries,
                buffsize,
                in_: PhantomData,
                out_: PhantomData,
            }
        }
    }

    impl<In, Out, Entry> PipelineEntry<In, Out> for Multiplex<In, Out, Entry>
        where Entry: PipelineEntry<In, Out> + Send + 'static,
              In: Send + 'static,
              Out: Send + 'static
    {
        fn process<I: IntoIterator<Item = In>>(self,
                                               rx: I,
                                               tx: mpsc::SyncSender<Out>) {
            // workers will read their work out of this channel but send their
            // results directly into the regular tx channel
            let (master_tx, chan_rx) = mpsc::sync_channel(self.buffsize);
            let chan_rx = LockedRx::new(chan_rx);

            for entry in self.entries {
                let entry_rx = chan_rx.clone();
                let entry_tx = tx.clone();

                thread::spawn(move || { entry.process(entry_rx, entry_tx); });
            }

            // now we copy the work from rx into the shared channel. the workers
            // will be putting their results into tx directly so this is the
            // only shuffling around that we have to do
            for item in rx {
                master_tx.send(item).expect("failed subsend");
            }
        }
    }

    struct LockedRx<T>
        where T: Send
    {
        lockbox: Arc<Mutex<mpsc::Receiver<T>>>,
    }

    impl<T> LockedRx<T>
        where T: Send
    {
        pub fn new(recv: mpsc::Receiver<T>) -> Self {
            Self { lockbox: Arc::new(Mutex::new(recv)) }
        }
    }

    impl<T> Clone for LockedRx<T>
        where T: Send
    {
        fn clone(&self) -> Self {
            Self { lockbox: self.lockbox.clone() }
        }
    }

    impl<T> Iterator for LockedRx<T>
        where T: Send
    {
        type Item = T;

        fn next(&mut self) -> Option<T> {
            match self.lockbox.lock().expect("failed unwrap mutex").recv() {
                Ok(val) => Some(val),
                Err(_recv_err) => {
                    // can only fail on hangup
                    None
                }
            }
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple() {
        let buffsize: usize = 10;
        let source: Vec<i32> = vec![1, 2, 3];
        let pbb: Pipeline<i32> = Pipeline::new(source, buffsize);
        let produced: Vec<i32> = pbb.into_iter().collect();

        assert_eq!(produced, vec![1, 2, 3]);
    }

    #[test]
    fn map() {
        let buffsize: usize = 10;

        let source: Vec<i32> = (1..1000).collect();
        let expect: Vec<i32> = source.iter().map(|x| x * 2).collect();

        let pbb: Pipeline<i32> =
            Pipeline::new(source, buffsize).map(|i| i * 2, buffsize);
        let produced: Vec<i32> = pbb.into_iter().collect();

        assert_eq!(produced, expect);
    }

    #[test]
    fn multiple_map() {
        let buffsize: usize = 10;
        let source: Vec<i32> = vec![1, 2, 3];
        let expect: Vec<i32> =
            source.iter().map(|x| (x * 2) * (x * 2)).collect();

        let pbb: Pipeline<i32> = Pipeline::new(source, buffsize)
            .map(|i| i * 2, buffsize)
            .map(|i| i * i, buffsize);
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
        // pointer and one that can take a closure. THis is the function pointer
        // side

        let buffsize: usize = 10;
        let workers: usize = 10;

        let source: Vec<u64> = (1..1000).collect();
        let expect: Vec<u64> =
            source.clone().into_iter().map(fib_work).collect();

        let pbb: Pipeline<u64> = Pipeline::new(source, buffsize)
            .then(multiplex::Multiplex::from(map::Mapper::new(fib_work),
                                             workers,
                                             buffsize),
                  buffsize);
        let mut produced: Vec<u64> = pbb.into_iter().collect();

        produced.sort(); // these may arrive out of order
        assert_eq!(produced, expect);
    }

    #[test]
    fn multiplex_map_closure() {
        let buffsize: usize = 10;
        let workers: usize = 10;

        let source: Vec<i32> = (1..1000).collect();
        let expect: Vec<i32> = source.iter().map(|x| x * 2).collect();

        let pbb: Pipeline<i32> = Pipeline::new(source, buffsize)
            .then(multiplex::Multiplex::new((0..workers)
                .map(|_| {
                        map::Mapper::new(|i| i * 2)
                    }).collect(),
                buffsize),
            buffsize);
        let mut produced: Vec<i32> = pbb.into_iter().collect();

        produced.sort(); // these may arrive out of order
        assert_eq!(produced, expect);
    }

    #[test]
    fn filter() {
        let buffsize: usize = 10;

        let source: Vec<i32> = (1..1000).collect();
        let expect: Vec<i32> = source
            .iter()
            .map(|x| x + 1)
            .filter(|x| x % 2 == 0)
            .collect();

        let pbb: Pipeline<i32> = Pipeline::new(source, buffsize)
            .map(|i| i + 1, buffsize)
            .filter(|i| i % 2 == 0, buffsize);
        let produced: Vec<i32> = pbb.into_iter().collect();

        assert_eq!(produced, expect);
    }

    #[test]
    fn simple_closure() {
        let buffsize: usize = 10;

        let source: Vec<i32> = (1..1000).collect();
        let expect: Vec<i32> = source
            .iter()
            .map(|x| x + 1)
            .filter(|x| x % 2 == 0)
            .collect();

        let pbb: Pipeline<i32> = Pipeline::new(source, buffsize)
            .pipe(|in_, out| for item in in_ {
                      let item = item + 1;
                      if item % 2 == 0 {
                          out.send(item).expect("failed to send")
                      }
                  },
                  10);
        let produced: Vec<i32> = pbb.into_iter().collect();

        assert_eq!(produced, expect);
    }
}
