A tool for constructing multi-threaded pipelines of execution

A `Pipeline` consists in one or more `PipelineEntry`s that each runs in its
own thread (or multiple threads in the case of `Multiplex`). They take in
items from the previous entry and produce items for the next entry, similar
to a Unix pipeline. This allows for expressing computation as a series of
steps that feed into each other and run concurrently

# Examples

Build the first 10 fibonacci numbers:

```rust
use pipelines::Pipeline;

let buffsize = 5;
fn fibonacci(n:u64)->u64{if n<2 {1} else {fibonacci(n-1) + fibonacci(n-2)}}

let nums: Vec<u64> = (0..10).collect();
let fibs: Vec<u64> = Pipeline::new(nums, buffsize)
    .map(fibonacci, 10)
    .into_iter().collect();
```

Build the first 10 fibonacci numbers in parallel:

```rust
use pipelines::Pipeline;
use pipelines::multiplex::Multiplex;
use pipelines::map::Mapper;

let buffsize = 5;
let workers = 2;
fn fibonacci(n:u64)->u64{if n<2 {1} else {fibonacci(n-1) + fibonacci(n-2)}}

let nums: Vec<u64> = (0..10).collect();
let fibs: Vec<u64> = Pipeline::new(nums, buffsize)
    .then(Multiplex::from(Mapper::new(fibonacci), workers, buffsize), buffsize)
    .into_iter().collect();
```