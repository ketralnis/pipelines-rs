#![feature(test)]extern crate test;

extern crate pipelines;

use pipelines::{Pipeline, Mapper, Multiplex};

const THREAD_COUNT: usize = 4;

// here we use "heavy" to mean a small number of expensive jobs and "light"
// to mean a large number of cheap jobs

// the heavy test is making sure that we get good thread utilisation
const HEAVY_WORK_COUNT: u64 = 10;
const HEAVY_WORK_FACTOR: u64 = 33;

// the light test is measuring how expensive all of the message passing is
const LIGHT_WORK_COUNT: u64 = 100_000;
const LIGHT_WORK_FACTOR: u64 = 9;

const BUFFSIZE: usize = 5;


#[bench]
fn bench_heavy_single(b: &mut test::Bencher) {
    b.iter(move || { single(HEAVY_WORK_COUNT, HEAVY_WORK_FACTOR); });
}


#[bench]
fn bench_light_single(b: &mut test::Bencher) {
    b.iter(move || { single(LIGHT_WORK_COUNT, LIGHT_WORK_FACTOR); });
}


#[bench]
fn bench_heavy_multi(b: &mut test::Bencher) {
    b.iter(move || { multi(HEAVY_WORK_COUNT, HEAVY_WORK_FACTOR); });
}


#[bench]
fn bench_light_multi(b: &mut test::Bencher) {
    b.iter(move || { multi(LIGHT_WORK_COUNT, LIGHT_WORK_FACTOR); });
}


fn single(work_count: u64, work_factor: u64) {
    let source: Vec<u64> = (1..work_count).collect();
    Pipeline::new(source, BUFFSIZE)
        .then(Mapper::new(move |_| fib_work(work_factor)), BUFFSIZE)
        .drain();
}


fn multi(work_count: u64, work_factor: u64) {
    let source: Vec<u64> = (1..work_count).collect();

    let mut entries = Vec::with_capacity(THREAD_COUNT);
    for _ in 0..THREAD_COUNT {
        let n = work_factor.clone();
        entries.push(Mapper::new(move |_| fib_work(n)))
    }

    Pipeline::new(source, BUFFSIZE)
        .then(Multiplex::new(entries, BUFFSIZE), BUFFSIZE)
        .drain();
}


// just something expensive
fn fib_work(n: u64) -> u64 {
    // fib scales as O(2^n) so `n` can be very coarse to tune
    fib(n) + n
}


fn fib(n: u64) -> u64 {
    if n == 0 || n == 1 {
        1
    } else {
        fib(n - 1) + fib(n - 2)
    }
}
