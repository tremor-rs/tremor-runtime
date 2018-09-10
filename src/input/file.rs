use input::{Input as InputT, INPUT_ERR, INPUT_OK};
use pipeline::Msg;
#[cfg(feature = "try_spmc")]
use spmc;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::sync::mpsc;

pub struct Input {
    file: String,
}

impl Input {
    pub fn new(opts: &str) -> Self {
        Self {
            file: String::from(opts),
        }
    }
}

impl InputT for Input {
    #[cfg(not(feature = "try_spmc"))]
    fn enter_loop(&mut self, pipelines: Vec<mpsc::SyncSender<Msg>>) {
        let reader = BufReader::new(File::open(self.file.clone()).unwrap());

        for (_num, line) in reader.lines().enumerate() {
            match line {
                Ok(line) => {
                    INPUT_OK.inc();
                    let msg = Msg::new(None, line);
                    pipelines[0].send(msg).unwrap();
                }
                Err(_) => INPUT_ERR.inc(),
            }
        }
    }

    #[cfg(feature = "try_spmc")]
    fn enter_loop2(&mut self, pipelines: Vec<spmc::Sender<Msg>>) {
        panic!("Not implemented");
    }
}
