extern crate clap;
extern crate pipelines;

use std::ffi::OsString;
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::io::stderr;
use std::process::{exit, Command, Stdio};
use std::thread;
use std::sync::Mutex;

use clap::{App, Arg};
use pipelines::{Pipeline, PipelineConfig};

fn main() {
    let app = App::new("mr_tools")
        .arg(Arg::with_name("nmappers")
                .short("M")
                .help("how many copies of the mapper to run")
                .takes_value(true)
                .default_value("1"))
        .arg(Arg::with_name("nreducers")
                .short("R")
                .help("how many copies of the reducer to run")
                .takes_value(true)
                .default_value("1"))
        .arg(Arg::with_name("field_sep")
                .short("F")
                .required(false)
                .default_value("\t")
                .help("field separator (used to group reducer keys; default is tab)"))
        .arg(Arg::with_name("mapper")
                .short("m")
                .long("map")
                .takes_value(true)
                .required(true)
                .help("command to run as mapper (executed with /bin/sh -c)"))
        .arg(Arg::with_name("reducer")
                .short("r")
                .long("reduce")
                .takes_value(true)
                .required(true)
                .help("command to run as reducer (executed with /bin/sh -c). each reducer also runs its own sorter"))
        .arg(Arg::with_name("sort_mem")
                .short("S")
                .takes_value(true)
                .help("-S argument to sort(1) (working memory set)"))
        .arg(Arg::with_name("sort_cmd")
                .long("sort")
                .takes_value(true)
                .help("the full sort(1) command to run (overrides -S)")
                );

    let arg_matches = app.get_matches();

    let mut u_field_sep = arg_matches.value_of("field_sep").unwrap_or("\t");
    if u_field_sep == "\\t" {
        // handle \t literal specially
        u_field_sep = "\t";
    }
    let field_sep_bytes = u_field_sep.as_bytes();
    if field_sep_bytes.len() > 1 {
        stderr()
            .write(b"field sep must be one byte")
            .expect("failed stderr");
        exit(1);
    }
    let field_sep = field_sep_bytes[0];

    let nmappers = arg_matches
        .value_of("nmappers")
        .unwrap_or("1")
        .parse::<usize>()
        .expect("bad -M integer");
    let nreducers = arg_matches
        .value_of("nreducers")
        .unwrap_or("1")
        .parse::<usize>()
        .expect("bad -R integer");

    let mapper = arg_matches
        .value_of_os("mapper")
        .expect("no mapper")
        .to_owned();
    let reducer = arg_matches
        .value_of_os("reducer")
        .expect("no reducer")
        .to_owned();

    let sort_cmd = {
        let sort_mem =
            arg_matches.value_of_os("sort_mem").map(|s| s.to_owned());
        let sort_cmd =
            arg_matches.value_of_os("sort_cmd").map(|s| s.to_owned());

        if sort_cmd.is_some() {
            sort_cmd.unwrap()
        } else {
            let mut sort_cmd = OsString::new();
            sort_cmd.push("LOCALE=C sort ");
            if sort_mem.is_some() {
                sort_cmd.push("-S");
                sort_cmd.push(sort_mem.unwrap());
            }
            sort_cmd
        }
    };
    // shadow it with a reference so multiple threads can all reference it
    let sort_cmd = Mutex::new(sort_cmd);

    // prime the pipeline with our stdin
    let pl =
        Pipeline::new(|tx| {
            let stdin = io::stdin();
            let locked = stdin.lock();
            for line in locked.split(b'\n') {
                // TODO check if result is eof?
                let line = line.expect("bad line in generator");
                tx.send(line);
            }
        }).configure(PipelineConfig::default().batch_size(100).buff_size(100));

    // execute the mappers
    let pl = pl.ppipe(nmappers, move |tx, rx| {
        let child = Command::new("/bin/sh")
            .arg("-c")
            .arg(&mapper)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .expect("failed spawn");
        let stdout = child.stdout.expect("no stdout?");
        let stdin = child.stdin.expect("no stdin?");

        // this thread reads off of the output of the child process and forwards it to the next
        // stage
        thread::spawn(move || {
            let buff = BufReader::new(stdout);
            for line in buff.split(b'\n') {
                let line = line.expect("no line?");
                let key_position = {
                    line.iter()
                        .position(|&x| x == field_sep)
                        .unwrap_or(line.len())
                };
                let key = line[..key_position].to_owned();
                tx.send((key, line));
            }
        });

        // now we need to send our input into the child process
        let mut buf = BufWriter::new(stdin);
        for line in rx {
            buf.write(&line).expect("failed mapper write");
            buf.write(b"\n").expect("failed mapper write");
        }
        buf.flush().expect("failed mapper flush");

        // now the output will all be headed to our tx so we're done here
    });

    // now execute the reducers. Each of our reducer threads will be sent all of the keys that
    // belong to it, but out of order. So to group them together before they hit the reducer
    // processes each will run its own sorter first

    let pl = pl.distribute(nreducers, move |tx, rx| {
        let mut reducer_cmd = OsString::new();
        {
            let locked = sort_cmd.lock().unwrap();
            reducer_cmd.push(&*locked);
        }
        reducer_cmd.push(" | ");
        reducer_cmd.push(&reducer);

        let child = Command::new("/bin/sh")
            .arg("-c")
            .arg(reducer_cmd)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .expect("failed spawn");
        let stdout = child.stdout.expect("no stdout?");
        let stdin = child.stdin.expect("no stdin?");

        // this thread reads off of the output of the child process and forwards it to the next
        // stage
        thread::spawn(move || {
            let buff = BufReader::new(stdout);
            for line in buff.split(b'\n') {
                let line = line.expect("no line?");
                tx.send(line);
            }
        });

        // now we need to send our input into the child process
        let mut buf = BufWriter::new(stdin);
        for (_key, line) in rx {
            buf.write(&line).expect("failed reducer write");
            buf.write(b"\n").expect("failed reducer write");
        }
        buf.flush().expect("failed reducer flush");

        // now the output will all be headed to our tx so we're done here
    });

    let stdout = io::stdout();
    let locked = stdout.lock();
    let mut buf = BufWriter::new(locked);

    // now we can just iterate the output of the final state and print it all
    for output in pl.into_iter() {
        // TODO broken pipe writes
        buf.write_all(&output).expect("failed write stdout");
        buf.write_all(b"\n").expect("failed newline stdout");
    }
    buf.flush().expect("failed final flush");
}
