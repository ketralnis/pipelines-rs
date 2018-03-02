#![feature(test)]
extern crate test;

extern crate pipelines;

use pipelines::{Pipeline, PipelineConfig};

const THREAD_COUNT: usize = 8;

// here we use "heavy" to mean a small number of expensive jobs and "light"
// to mean a large number of cheap jobs

// the heavy test is making sure that we get good thread utilisation
const HEAVY_WORK_COUNT: u64 = 10;
const HEAVY_WORK_FACTOR: u64 = 31;
const HEAVY_BATCH_SIZE: usize = 1;

// the light test is measuring how expensive all of the message passing is
const LIGHT_WORK_COUNT: u64 = 50_000;
const LIGHT_WORK_FACTOR: u64 = 9;
const LIGHT_BATCH_SIZE: usize = 100;

#[bench]
fn bench_heavy_single(b: &mut test::Bencher) {
    b.iter(move || {
        single(HEAVY_WORK_COUNT, HEAVY_WORK_FACTOR, HEAVY_BATCH_SIZE);
    });
}

#[bench]
fn bench_light_single(b: &mut test::Bencher) {
    b.iter(move || {
        single(LIGHT_WORK_COUNT, LIGHT_WORK_FACTOR, LIGHT_BATCH_SIZE);
    });
}

#[bench]
fn bench_heavy_multi(b: &mut test::Bencher) {
    b.iter(move || {
        multi(HEAVY_WORK_COUNT, HEAVY_WORK_FACTOR, HEAVY_BATCH_SIZE);
    });
}

#[bench]
fn bench_light_multi(b: &mut test::Bencher) {
    b.iter(move || {
        multi(LIGHT_WORK_COUNT, LIGHT_WORK_FACTOR, LIGHT_BATCH_SIZE);
    });
}

fn single(work_count: u64, work_factor: u64, batch_size: usize) {
    let source: Vec<u64> = (1..work_count).collect();
    Pipeline::from(source)
        .configure(PipelineConfig::default().batch_size(batch_size))
        .map(move |_| fib_work(work_factor))
        .reduce(|_, nums| *nums.iter().max().unwrap())
        .drain();
}

fn multi(work_count: u64, work_factor: u64, batch_size: usize) {
    let source: Vec<u64> = (1..work_count).collect();
    Pipeline::from(source)
        .configure(PipelineConfig::default().batch_size(batch_size))
        .pmap(THREAD_COUNT, move |_| fib_work(work_factor))
        .preduce(THREAD_COUNT, |_, nums| *nums.iter().max().unwrap())
        .drain();
}

// just something expensive. it's a tuple so we can reduce on it
fn fib_work(n: u64) -> (bool, u64) {
    // fib scales as O(2^n) so `n` can be very coarse to tune
    let computed = fib(n) + n;
    (computed % 2 == 0, computed)
}

fn fib(n: u64) -> u64 {
    if n == 0 || n == 1 {
        1
    } else {
        fib(n - 1) + fib(n - 2)
    }
}
