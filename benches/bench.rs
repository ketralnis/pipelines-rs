#![feature(test)]
extern crate test;

extern crate pipelines;

use pipelines::multiplex;
use pipelines::map;
use pipelines::Pipeline;

// tuneables for different workloads
const THREAD_COUNT: usize = 2;
const WORK_COUNT: u64 = 10;
const WORK_FACTOR: u64 = 33; // fib scales as O(2^n)
const BUFFSIZE: usize = 5;


#[bench]
fn bench_single(b: &mut test::Bencher) {
    b.iter(move || {
        let source: Vec<u64> = (1..WORK_COUNT).collect();
        Pipeline::new(source, 5).map(fib_work, BUFFSIZE).drain();
    });
}


#[bench]
fn bench_multi(b: &mut test::Bencher) {
    b.iter(move || {
        let source: Vec<u64> = (1..WORK_COUNT).collect();

        Pipeline::new(source, BUFFSIZE)
            .then(multiplex::Multiplex::from(map::Mapper::new(fib_work),
                                            THREAD_COUNT,
                                            BUFFSIZE),
                BUFFSIZE)
            .drain();
    });
}


// just something expensive
fn fib_work(n: u64) -> u64 {
    fib(WORK_FACTOR) + n
}


fn fib(n: u64) -> u64 {
    if n == 0 || n == 1 {
        1
    } else {
        fib(n - 1) + fib(n - 2)
    }
}
