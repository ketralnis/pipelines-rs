extern crate time;

extern crate rs_ducts;

use rs_ducts::multiplex;
use rs_ducts::map;

use rs_ducts::Pipeline;

// tuneables for different workloads
const THREAD_COUNT: usize = 1000;
const WORK_COUNT: u64 = 1000;
const WORK_FACTOR: u64 = 34;
const BUFFSIZE: usize = 5;

fn bench_single() {
    let source: Vec<u64> = (1..WORK_COUNT).collect();

    Pipeline::new(source, 5).map(fib_work, BUFFSIZE).drain();
}


fn bench_multi() {
    let source: Vec<u64> = (1..WORK_COUNT).collect();

    Pipeline::new(source, BUFFSIZE)
        .then(multiplex::Multiplex::from(map::Mapper::new(fib_work),
                                         THREAD_COUNT,
                                         BUFFSIZE),
              BUFFSIZE)
        .drain();
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


pub fn timeit<F>(name: &str, func: F)
    where F: FnOnce() -> () + Copy
{
    println!("Starting {}", name);
    let started = time::precise_time_ns();
    func();
    let took = time::precise_time_ns() - started;
    println!("Completed {} in {}ns ({:.4}s)",
             name,
             took,
             (took as f64) / 1_000_000_000.0);
}


pub fn main() {
    timeit("single", bench_single);
    timeit("multi", bench_multi);
}
