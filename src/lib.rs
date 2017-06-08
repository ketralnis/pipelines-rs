extern crate chan;

use std::sync::mpsc;
use std::thread;

#[derive(Debug)]
pub struct Pipeline<Output>
        where Output: Send + 'static {
    rx: mpsc::Receiver<Output>,
}

impl<Output> Pipeline<Output>
        where Output: Send {

    // start up the producer thread and start sending items into rx
    #[must_use]
    pub fn new<I>(source: I, buffsize: usize)
            -> Pipeline<Output>
            where I: Send + 'static + IntoIterator<Item=Output> {
        let (tx, rx) = mpsc::sync_channel(buffsize);
        thread::spawn(move || {
            for item in source {
                tx.send(item).expect("failed send (super)");
            }
        });

        Pipeline{rx}
    }

    // given another pipeline entry, send the results of the previous entry into
    // the next one
    #[must_use]
    pub fn then<EntryOut, Entry>(self, next: Entry, buffsize: usize)
            -> Pipeline<EntryOut>
            where Entry: Send + 'static + PipelineEntry<Output, EntryOut>,
                  EntryOut: Send {
        let (tx, rx) = mpsc::sync_channel(buffsize);
        thread::spawn(move || {
            next.process(self.rx, tx);
        });

        Pipeline{rx}
    }

    #[must_use]
    pub fn into_iter(self) -> mpsc::IntoIter<Output> {
        self.rx.into_iter()
    }

    pub fn drain(self) {
        for _ in self.into_iter() {}
    }
}

pub trait PipelineEntry<In, Out> {
    fn process<I: IntoIterator<Item=In>>(self, rx: I, tx: mpsc::SyncSender<Out>) -> ();
}

pub mod map {
    use std::marker::PhantomData;
    use std::sync::mpsc::SyncSender;

    use super::PipelineEntry;

    #[derive(Debug)]
    pub struct Mapper<In, Out, Func>
            where Func: Fn(In) -> Out {
        func: Func,

        // make the compiler happy
        in_: PhantomData<In>,
        out_: PhantomData<Out>,
    }

    impl<In, Out, Func> Mapper<In, Out, Func>
            where Func: Fn(In) -> Out {
        pub fn new(func: Func) -> Self {
            Mapper{func,
                   in_: PhantomData,
                   out_: PhantomData}
        }
    }

    impl<In, Out, Func> PipelineEntry<In, Out> for Mapper<In, Out, Func>
            where Func: Fn(In) -> Out {
        fn process<I: IntoIterator<Item=In>>(self, rx: I, tx: SyncSender<Out>) {
            for item in rx {
                let mapped = (self.func)(item);
                tx.send(mapped).expect("failed to send");
            }
        }
    }
}

pub mod multiplex {
    use std::marker::PhantomData;
    use std::sync::mpsc;
    use std::thread;

    use chan;

    use super::PipelineEntry;

    #[derive(Debug)]
    pub struct Multiplex<In, Out, Entry>
            where Entry: PipelineEntry<In, Out> {
        entries: Vec<Entry>,
        buffsize: usize,

        // make the compiler happy
        in_: PhantomData<In>,
        out_: PhantomData<Out>,
    }

    impl<In, Out, Entry> Multiplex<In, Out, Entry>
            where Entry: PipelineEntry<In, Out> + Send {
        pub fn new(entries: Vec<Entry>, buffsize: usize) -> Self {
            Multiplex{entries,
                      buffsize,
                      in_: PhantomData,
                      out_: PhantomData}
        }
    }

    impl<In, Out, Entry> PipelineEntry<In, Out> for Multiplex<In, Out, Entry>
            where Entry: PipelineEntry<In, Out> + Send + 'static,
                  In: Send + 'static,
                  Out: Send + 'static {
        fn process<I: IntoIterator<Item=In>>(self, rx: I, tx: mpsc::SyncSender<Out>) {
            // workers will read their work out of this channel but send their
            // results directly into the regular tx channel. We pull in the chan
            // package for this rather than use mpsc because we need multiple
            // consumers
            let (chan_tx, chan_rx) = chan::sync(self.buffsize);

            for entry in self.entries {
                let entry_rx = chan_rx.clone();
                let entry_tx = tx.clone();
                thread::spawn(move || {
                    entry.process(entry_rx, entry_tx);
                });
            }

            // now we copy the work from rx into the shared channel. the workers
            // will be putting their results into tx directly so this is the
            // only shuffling around that we have to do
            for item in rx {
                chan_tx.send(item);
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
        let pbb: Pipeline<i32> = Pipeline::new(source.clone(), buffsize);
        let produced: Vec<i32> = pbb.into_iter().collect();

        assert_eq!(produced, vec![1, 2, 3]);
    }

    #[test]
    fn map() {
        let buffsize: usize = 10;

        let source: Vec<i32> = (1..1000).collect();
        let expect: Vec<i32> = source.iter().map(|x| x*2).collect();

        let pbb: Pipeline<i32> = Pipeline::new(source.clone(), buffsize)
            .then(map::Mapper::new(|i| i*2), buffsize);
        let produced: Vec<i32> = pbb.into_iter().collect();

        assert_eq!(produced, expect);
    }

    #[test]
    fn multiple_map() {
        let buffsize: usize = 10;
        let source: Vec<i32> = vec![1, 2, 3];
        let expect: Vec<i32> = source.iter().map(|x| (x*2)*(x*2)).collect();

        let pbb: Pipeline<i32> = Pipeline::new(source.clone(), buffsize)
            .then(map::Mapper::new(|i| i*2), buffsize)
            .then(map::Mapper::new(|i| i*i), buffsize);
        let produced: Vec<i32> = pbb.into_iter().collect();

        assert_eq!(produced, expect);
    }

    #[test]
    fn multiplex_map() {
        let buffsize: usize = 10;

        let source: Vec<i32> = (1..1000).collect();
        let expect: Vec<i32> = source.iter().map(|x| x*2).collect();

        let pbb: Pipeline<i32> = Pipeline::new(source.clone(), buffsize)
            // TOOD multiplex takes a list of PipelineEntry but it would be
            // nicer if it just took one and was able to clone it
            .then(
                multiplex::Multiplex::new(
                    (0..10).map(|_| map::Mapper::new(|i| i*2)).collect(),
                    buffsize),
                buffsize);
        let mut produced: Vec<i32> = pbb.into_iter().collect();

        produced.sort(); // these may arrive out of order
        assert_eq!(produced, expect);
    }
}
