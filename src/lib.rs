use std::sync::mpsc::Receiver;
use std::sync::mpsc::sync_channel;
use std::sync::mpsc::SyncSender;
use std::thread;
use std::sync::mpsc;



pub struct Pipeline<Output>
        where Output: Send + 'static {
    rx: Receiver<Output>,
}

impl<Output> Pipeline<Output>
        where Output: Send + 'static {

    // start up the producer thread and start sending items into rx
    #[must_use]
    pub fn new<I>(source: I, buffsize: usize)
            -> Pipeline<Output>
            where I: Send + 'static + IntoIterator<Item=Output> {
        let (tx, rx) = sync_channel(buffsize);
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
                  EntryOut: Send + 'static {
        let (tx, rx) = sync_channel(buffsize);
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
    fn process(self, rx: Receiver<In>, tx: SyncSender<Out>) -> ();
}

pub mod map {
    use std::sync::mpsc::SyncSender;
    use std::sync::mpsc::Receiver;
    use std::marker::PhantomData;

    use super::PipelineEntry;

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
        fn process(self, rx: Receiver<In>, tx: SyncSender<Out>) {
            for item in rx {
                let mapped = (self.func)(item);
                tx.send(mapped).expect("failed to send");
            }
        }
    }
}

// pub mod multiplex {
//     use super::PipelineEntry;

//     use std::sync::mpsc::SyncSender;
//     use std::sync::mpsc::Receiver;
//     use std::marker::PhantomData;

//     use super::PipelineEntry;

//     pub struct Multiplex<In, Out, Entry>
//             where Entry: PipelineEntry<In, Out> + Clone + Send + 'static {
//         entry: Entry
//     }

//     impl<In, Out, Func> Multiplex<In, Out, Func>
//             where Func: Fn(In) -> Out {
//         pub fn new(func: Func) -> Self {
//             Multiplex{func,
//                    in_: PhantomData,
//                    out_: PhantomData}
//         }
//     }

//     impl<In, Out, Func> PipelineEntry<In, Out> for Multiplex<In, Out, Func>
//             where Func: Fn(In) -> Out {
//         fn process(self, rx: Receiver<In>, tx: SyncSender<Out>) {
//             for item in rx {
//                 let mapped = (self.func)(item);
//                 tx.send(mapped).expect("failed to send");
//             }
//         }
//     }
// }


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
        let source: Vec<i32> = vec![1, 2, 3];
        let pbb: Pipeline<i32> = Pipeline::new(source.clone(), buffsize)
            .then(map::Mapper::new(|i| i*2), buffsize);
        let produced: Vec<i32> = pbb.into_iter().collect();

        assert_eq!(produced, vec![2, 4, 6]);
    }

    #[test]
    fn multiple_map() {
        let buffsize: usize = 10;
        let source: Vec<i32> = vec![1, 2, 3];
        let pbb: Pipeline<i32> = Pipeline::new(source.clone(), buffsize)
            .then(map::Mapper::new(|i| i*2), buffsize)
            .then(map::Mapper::new(|i| i*i), buffsize);
        let produced: Vec<i32> = pbb.into_iter().collect();

        assert_eq!(produced, vec![4, 16, 36]);
    }
}
