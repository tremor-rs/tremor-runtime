use input::{Input as InputT, INPUT_ERR, INPUT_OK};
use pipeline::{Context, Msg};
use serde_json;
use std::ffi::OsStr;
use std::fs::File;
use std::io::{BufRead, Read};
use std::path::Path;
use std::sync::mpsc;
use std::time::Duration;
use utils::{nanotime, park};
use xz2::read::XzDecoder;

#[cfg(feature = "try_spmc")]
use spmc;

pub struct Input {
    config: Config,
    data: Vec<u8>,
    next: u64,
}

#[derive(Deserialize, Debug, Clone)]
struct Config {
    source: String,
    interval: u64,
    warmup_iters: u64,
    iters: u64,
}

impl Input {
    pub fn new(opts: &str) -> Input {
        match serde_json::from_str(opts) {
            Ok(config @ Config { .. }) => {
                let mut source_data_file = File::open(&config.source).unwrap();
                {
                    let ext = Path::new(&config.source).extension().and_then(OsStr::to_str).unwrap();

                    let mut raw = vec![];
                    if ext == "xz" {
                        XzDecoder::new(source_data_file).read_to_end(&mut raw).expect("Neither a readable nor valid XZ compressed file error");
                    } else {
                        source_data_file.read_to_end(&mut raw).expect("Unable to read data source file error");
                    }
                    Input { config: config.clone(), data: raw, next: nanotime() }
                }
            }
            e => {
                panic!("Invalid options for Blaster input, use `{{\"source\": \"<path/to/file.json|path/to/file.json.xz>\", \"is_coordinated\": [true|false], \"rate\": <rate>}}`\n{:?} ({})", e, opts)
            }
        }
    }

    pub fn step(&mut self, pipelines: &[mpsc::SyncSender<Msg>], ctx: Option<Context>) {
        {
            for line in self.data.lines() {
                match line {
                    Ok(line) => {
                        INPUT_OK.inc();
                        while nanotime() < self.next {
                            park(Duration::from_nanos(1));
                        }
                        self.next += self.config.interval;
                        let msg = match ctx {
                            Some(_) => Msg::new_with_context(None, line, ctx),
                            None => Msg::new(None, line),
                        };
                        let _ = pipelines[0].send(msg);
                    }
                    Err(_) => INPUT_ERR.inc(),
                }
            }
        }
    }
}

impl InputT for Input {
    #[cfg(not(feature = "try_spmc"))]
    fn enter_loop(&mut self, pipelines: Vec<mpsc::SyncSender<Msg>>) {
        let warmup = Some(Context { warmup: true });
        let measured = Some(Context { warmup: false });

        self.next = nanotime() + self.config.interval;
        for _ in 0..self.config.warmup_iters {
            self.step(&pipelines, warmup);
        }

        self.next = nanotime() + self.config.interval;
        for _ in 0..self.config.iters {
            self.step(&pipelines, measured);
        }
    }
    #[cfg(feature = "try_spmc")]
    fn enter_loop2(&mut self, pipelines: Vec<spmc::Sender<Msg>>) {
        println!("TBD - enter_loop2 i inputt");
    }
}
