/// A `pipelines` example to see how much space could be saved in a directory
/// with compression
#[macro_use]
extern crate log;
extern crate env_logger;
extern crate flate2;
extern crate humansize;
extern crate num_cpus;
extern crate walkdir;

extern crate pipelines;

use std::env;
use std::ffi::OsString;
use std::fs::File;
use std::io::Cursor;
use std::io::Read;

use flate2::Compression;
use flate2::read::ZlibEncoder;
use humansize::{FileSize, file_size_opts};

use pipelines::{Pipeline, Mapper, Multiplex};

fn main() {
    // we could have this many whole files' contents in memory at once
    const BUFFSIZE: usize = 10;

    // how many threads to use for compression
    let workers = num_cpus::get();

    let args: Vec<OsString> = env::args_os().skip(1).collect();

    let pl = Pipeline::from(args, 0)
        .pipe(|args, out| {
            // walk all of the directories we were passed
            for arg in args {
                debug!("Walking into {:?}", arg);
                let entries = walkdir::WalkDir::new(arg).into_iter()
                    .filter_map(|e| e.ok()) // silently skip stuff we can't read
                    .filter(|e| e.file_type().is_file());
                for entry in entries {
                    debug!("Walked into {:?}", entry);
                    // let entry = entry.expect("bad entry");
                    let metadata = entry.metadata().expect("bad stat");
                    let len = metadata.len();
                    if len > 0 {
                        out.send((entry.path().to_owned(), len))
                            .expect("bad send");
                    }
                }
            }
        }, BUFFSIZE)
        .map(|(fname, file_len)| {
            // open up each file and read out the data. it's probably only
            // useful to have one of these going at a time if the files are all
            // on the same disk
            let mut file = File::open(&fname).expect("failed to read");
            let mut data = Vec::with_capacity(file_len as usize);
            file.read_to_end(&mut data).expect("couldn't read");
            debug!("Read {:?}: {} bytes", fname, data.len());
            data
        }, BUFFSIZE)
        // but we can do the compression in parallel
        .then(Multiplex::from(Mapper::new(try_compress),
                              workers,
                              BUFFSIZE),
              BUFFSIZE);

    let mut total_old_size: usize = 0;
    let mut total_new_size: usize = 0;
    let mut size_diff: isize = 0;
    let mut total_files: u64 = 0;

    for (old_size, new_size) in pl {
        total_old_size += old_size;
        total_new_size += new_size;
        size_diff += old_size as isize - new_size as isize;
        total_files += 1;
    }

    println!(
        "You could save {} with compression ({} -> {} in {} files)",
        friendly(size_diff),
        friendly(total_old_size as isize),
        friendly(total_new_size as isize),
        total_files,
    )
}

fn friendly(size: isize) -> String {
    size.file_size(file_size_opts::CONVENTIONAL)
        .expect("bad file size")
}

fn try_compress(data: Vec<u8>) -> (usize, usize) {
    let old_size = data.len();
    let in_data = Cursor::new(data);
    let mut out_data = Vec::with_capacity(old_size);
    let mut compressor = ZlibEncoder::new(in_data, Compression::Best);
    compressor.read_to_end(&mut out_data).expect("bad compress");
    let new_size = out_data.len();
    debug!("Compressed {} to {}", old_size, new_size);
    (old_size, new_size)
}
