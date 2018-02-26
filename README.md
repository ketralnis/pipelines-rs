A tool for constructing multi-threaded pipelines of execution

A `Pipeline` consists in one or more stages that each runs in its own thread (or multiple
threads). They take in items from the previous stage and produce items for the next stage,
similar to a Unix pipeline. This allows for expressing computation as a series of steps that
feed into each other and run concurrently

# Examples

Build the first 10 fibonacci numbers:

```rust
use pipelines::Pipeline;

fn fibonacci(n:u64)->u64{if n<2 {1} else {fibonacci(n-1) + fibonacci(n-2)}}

let nums: Vec<u64> = (0..10).collect();
let fibs: Vec<u64> = Pipeline::from(nums)
    .map(fibonacci)
    .into_iter().collect();
```

Build the first 10 fibonacci numbers in parallel, then double them:

```rust
use pipelines::Pipeline;

let workers = 2;
fn fibonacci(n:u64)->u64{if n<2 {1} else {fibonacci(n-1) + fibonacci(n-2)}}

let nums: Vec<u64> = (0..10).collect();
let fibs: Vec<u64> = Pipeline::from(nums)
    .pmap(workers, fibonacci)
    .map(|x| x*2)
    .into_iter().collect();
```

Build the first 10 fibonacci numbers in parallel then group them by evenness, expressed in
mapreduce stages

```rust
use pipelines::Pipeline;

let workers = 2;
fn fibonacci(n:u64)->u64{if n<2 {1} else {fibonacci(n-1) + fibonacci(n-2)}}

let nums: Vec<u64> = (0..10).collect();
let fibs: Vec<(bool, u64)> = Pipeline::from(nums)
    .pmap(workers, fibonacci)
    .map(|num| (num % 2 == 0, num))
    .preduce(workers, |evenness, nums| (evenness, *nums.iter().max().unwrap()))
    .into_iter().collect();
```
