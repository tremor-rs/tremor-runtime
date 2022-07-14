// Copyright 2020-2022, The Tremor Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
use clap::{Parser, ValueEnum};

/// Tremor cli - Command Line Interface
#[derive(Parser, Debug)]
#[clap(name = "tremor", author, version)]
pub(crate) struct Cli {
    /// Instance identifier
    #[clap(short, long, default_value = "tremor", value_parser = clap::value_parser!(String))]
    pub(crate) instance: String,
    /// Configuration for Log4RS
    #[clap(short, long, value_parser = clap::value_parser!(String))]
    pub(crate) logger_config: Option<String>,
    #[clap(subcommand)]
    pub(crate) command: Command,
}

#[derive(Parser, Debug)]
pub(crate) enum Command {
    /// Generate shell completions to stdout. Tries to guess the shell if no subcommand is given.
    Completions {
        #[clap(value_enum, value_parser = clap::value_parser!(clap_complete::shells::Shell))]
        shell: Option<clap_complete::shells::Shell>,
    },
    /// Tremor server
    Server {
        #[clap(subcommand)]
        command: ServerCommand,
    },
    /// Testing facilities
    Test(Test),
    /// Advanced debugging commands
    Dbg(Dbg),
    /// Run tremor script or query files against stdin or a json data archive,
    /// the data will be read from STDIN or an archive and written to STDOUT.
    Run(Run),
    /// Generates documention from tremor script files
    Doc(Doc),
    /// Creates a template tremor project
    New {
        #[clap( value_parser = clap::value_parser!(String))]
        name: String,
    },

    /// cluster commands
    Cluster(Cluster),
}

#[derive(Parser, Debug)]
pub(crate) struct Cluster {
    #[clap(subcommand)]
    pub(crate) command: ClusterCommand,
}

#[derive(Parser, Clone, Debug)]
pub(crate) enum ClusterCommand {
    /// Initializes a new initial cluster
    Init {
        /// Database dir to store raft data in
        #[clap(short, long, value_parser = clap::value_parser!(String))]
        db_dir: String,
        /// Raft RPC IP and endpoint to listen to
        #[clap(short, long, value_parser = clap::value_parser!(String))]
        rpc: String,
        /// Raft API IP and endpoint to listen to
        #[clap(short, long, value_parser = clap::value_parser!(String))]
        api: String,
    },

    /// Joins a new node to the cluster
    Join {
        /// Database dir to store raft data in
        #[clap(short, long, value_parser = clap::value_parser!(String))]
        db_dir: String,
        /// The cluster to join, defaults to `TREMER_API_ADDRESS`
        #[clap(short, long, value_parser = clap::value_parser!(String))]
        cluster_api: Option<String>,
        /// Raft RPC IP and endpoint to listen to
        #[clap(short, long, value_parser = clap::value_parser!(String))]
        rpc: String,
        /// Raft API IP and endpoint to listen to
        #[clap(short, long, value_parser = clap::value_parser!(String))]
        api: String,
    },
    /// Starts a cluster node that is part as a cluster already
    Start {
        /// Database dir to store raft data in
        #[clap(short, long, value_parser = clap::value_parser!(String))]
        db_dir: String,
    },
    /// Removes a note from the cluster (the node still has to be stopped after this)
    Remove {
        /// Raft API IP and endpoint to send to
        #[clap(short, long, value_parser = clap::value_parser!(String))]
        api: Option<String>,
        /// Node to remove
        #[clap(value_parser = clap::value_parser!(u64))]
        node: u64,
    },
    /// Displays the cluster status
    Status {
        /// Raft API IP and endpoint to send to
        #[clap(short, long, value_parser = clap::value_parser!(String))]
        api: Option<String>,
        /// Show JSON output instead of human readable information
        #[clap(short, long, action = clap::ArgAction::SetTrue)]
        json: bool,
    },
    /// App related tommands
    Apps {
        /// Raft API IP and endpoint to send to, defaults to `TREMER_API_ADDRESS`
        #[clap(short, long, value_parser = clap::value_parser!(String))]
        api: Option<String>,
        #[clap(subcommand)]
        command: AppsCommands,
    },
    /// Packages a tremor application
    Package {
        /// the file to load
        #[clap(short, long, value_parser = clap::value_parser!(String))]
        name: Option<String>,
        #[clap(short, long, value_parser = clap::value_parser!(String))]
        out: String,
        #[clap( value_parser = clap::value_parser!(String))]
        entrypoint: String,
    },
}

#[derive(Parser, Clone, Debug)]
pub(crate) enum AppsCommands {
    /// lists all installed apps
    List {
        /// Show JSON output instead of human readable information
        #[clap(short, long, action = clap::ArgAction::SetTrue)]
        json: bool,
    },
    /// Installs  new app
    Install {
        /// the file to load
        #[clap( value_parser = clap::value_parser!(String))]
        file: String,
    },
    /// Starts an instance of a flow in an installed app
    Start {
        /// The app to start
        #[clap(value_parser = clap::value_parser!(String))]
        app: String,
        /// the instance to start
        #[clap(value_parser = clap::value_parser!(String))]
        instance: String,
        /// The flow to start defaults to main
        #[clap(long, short, value_parser = clap::value_parser!(String))]
        flow: Option<String>,
        /// Data
        #[clap(long, short, value_parser = clap::value_parser!(String))]
        config: Option<String>,
    },
}

/// Shell type
#[derive(ValueEnum, Clone, Copy, Debug)]
pub(crate) enum Shell {
    /// Generate completion based on active shell
    Guess,
    /// Generate bash shell completions
    Bash,
    /// Generate zsh shell completions
    Zsh,
    /// Generate elvish shell completions
    Elvish,
    /// Generate fish shell completions
    Fish,
    /// Generate powershell shell completions
    Powershell,
}

#[derive(Parser, Debug)]
pub(crate) struct Test {
    /// Specifies what to test
    #[clap(value_enum, default_value_t, value_parser = clap::value_parser!(TestMode))]
    pub(crate) mode: TestMode,
    /// The root test path
    #[clap(default_value = "tests", value_parser = clap::value_parser!(String))]
    pub(crate) path: String,
    /// Should generate a test report to specified path
    #[clap(short = 'o', long, value_parser = clap::value_parser!(String))]
    pub(crate) report: Option<String>,
    /// Optional tags to filter test incusions by
    #[clap(short, long, value_parser = clap::value_parser!(String))]
    pub(crate) includes: Vec<String>,
    /// Optional tags to filter test incusions by
    #[clap(short, long, value_parser = clap::value_parser!(String))]
    pub(crate) excludes: Vec<String>,
    /// Sets the level of verbosity (does not apply to logging)
    #[clap(short, long, action = clap::ArgAction::SetTrue)]
    pub(crate) verbose: bool,
    /// Timeout in seconds for each test
    #[clap(short, long, value_parser = clap::value_parser!(u64))]
    pub(crate) timeout: Option<u64>,
}

/// Shell type
#[derive(ValueEnum, Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
pub(crate) enum TestMode {
    /// Run all tests
    All,
    /// Run benchmarks
    Bench,
    /// Run command tests
    Command,
    /// Run integration tests
    Integration,
    /// Run tremor script unit tests
    Unit,
}

impl ToString for TestMode {
    fn to_string(&self) -> String {
        match self {
            TestMode::Bench => "bench".to_string(),
            TestMode::Integration => "integration".to_string(),
            TestMode::Command => "command".to_string(),
            TestMode::Unit => "unit".to_string(),
            TestMode::All => "all".to_string(),
        }
    }
}
impl Default for TestMode {
    fn default() -> Self {
        Self::All
    }
}

#[derive(Parser, Debug)]
pub(crate) struct Doc {
    /// Generates and prints to standard output
    #[clap(short, long, action = clap::ArgAction::SetTrue)]
    pub(crate) interactive: bool,
    #[clap(value_parser = clap::value_parser!(String))]
    /// Directory or source to generate documents for
    pub(crate) dir: String,
    #[clap(default_value = "docs", value_parser = clap::value_parser!(String))]
    pub(crate) outdir: String,
}

#[derive(Parser, Debug)]
pub(crate) struct Run {
    #[clap(value_parser = clap::value_parser!(String))]
    /// filename to run the data through
    pub(crate) script: String,
    /// Should not output to consumed source / produced synthetic data or errors
    #[clap(long, action = clap::ArgAction::SetTrue)]
    pub(crate) interactive: bool,
    /// Should not pretty print data [ when in interactive mode ]
    #[clap(long, action = clap::ArgAction::SetTrue)]
    pub(crate) pretty: bool,
    /// The codec to use for encoding the data
    #[clap(short, long, default_value = "json", value_parser = clap::value_parser!(String))]
    pub(crate) encoder: String,
    /// The codec to use for decoding the data
    #[clap(short, long, default_value = "json", value_parser = clap::value_parser!(String))]
    pub(crate) decoder: String,
    /// Input file
    #[clap(short, long, default_value = "-", value_parser = clap::value_parser!(String))]
    pub(crate) infile: String,
    /// Output file
    #[clap(short, long, default_value = "-", value_parser = clap::value_parser!(String))]
    pub(crate) outfile: String,
    /// Preprocessors to pass data through before decoding
    #[clap(long, default_value = "separate", value_parser = clap::value_parser!(String))]
    pub(crate) preprocessor: String,
    /// Postprocessor to pass data through after encoding
    #[clap(long, default_value = "separate", value_parser = clap::value_parser!(String))]
    pub(crate) postprocessor: String,
    /// Specifies the port that is printed to the output
    #[clap(short, long, value_parser = clap::value_parser!(String))]
    pub(crate) port: Option<String>,
}

#[derive(Parser, Debug)]
pub(crate) struct DbgSrc {
    /// output the pre-processed source
    #[clap(short, long, action = clap::ArgAction::SetTrue)]
    pub(crate) preprocess: bool,
    /// tremor/json/trickle/troy File
    #[clap(value_parser = clap::value_parser!(String))]
    pub(crate) script: String,
}

#[derive(Parser, Debug)]
pub(crate) struct DbgAst {
    /// only prints the expressions
    #[clap(short = 'x', long, action = clap::ArgAction::SetTrue)]
    pub(crate) exprs_only: bool,
    #[clap(value_parser = clap::value_parser!(String))]
    /// tremor/json/trickle/troy File
    pub(crate) script: String,
}

#[derive(Parser, Debug)]
pub(crate) struct DbgDot {
    #[clap(value_parser = clap::value_parser!(String))]
    /// tremor/json/trickle/troy File
    pub(crate) script: String,
}
#[derive(Parser, Debug)]
pub(crate) enum DbgCommand {
    /// Prints the .dot representation for a trickle file (you can use `| dot -Tpng -oout.png` to generate a picture)
    Dot(DbgDot),
    /// Prints the AST of the source
    Ast(DbgAst),
    /// Prints Lexemes
    Lex(DbgSrc),
    /// Prints source
    Src(DbgSrc),
}

#[derive(Parser, Debug, Clone, Copy)]
pub(crate) struct DbgOpts {
    /// Do not print the banner
    #[clap(short = 'b', long, action = clap::ArgAction::SetTrue)]
    pub(crate) no_banner: bool,
    /// Do not highlight output
    #[clap(short = 'n', long, action = clap::ArgAction::SetTrue)]
    pub(crate) no_highlight: bool,
    /// Do not output any formatting. Disables highlight, banner, line numbers.
    #[clap(short, long, action = clap::ArgAction::SetTrue)]
    pub(crate) raw: bool,
}

#[derive(Parser, Debug)]
pub(crate) struct Dbg {
    #[clap(flatten)]
    pub opts: DbgOpts,
    #[clap(subcommand)]
    pub command: DbgCommand,
}

#[derive(Parser, Debug)]
pub(crate) enum ServerCommand {
    /// Runs the tremor server process
    Run(ServerRun),
}

#[derive(Parser, Debug)]
pub(crate) struct ServerRun {
    #[clap(value_parser = clap::value_parser!(String))]
    /// Paths to files containing pipelines, onramps, offramps to provision
    pub(crate) artefacts: Vec<String>,
    /// Captures process id if set and stores in a file
    #[clap(short, long, value_parser = clap::value_parser!(String))]
    pub(crate) pid: Option<String>,
    /// Disable the API
    #[clap(short, long, action = clap::ArgAction::SetTrue)]
    pub(crate) no_api: bool,
    /// Loads the debug connectors
    #[clap(short, long, action = clap::ArgAction::SetTrue)]
    pub(crate) debug_connectors: bool,
    /// The `host:port` to listen for the API
    #[clap(short, long, default_value = "0.0.0.0:9898", value_parser = clap::value_parser!(String))]
    pub(crate) api_host: String,
    /// function tail-recursion stack depth limit
    #[clap(short, long, default_value = "1024", value_parser = clap::value_parser!(u32))]
    pub(crate) recursion_limit: u32,
}

// TODO: since the API will change this isn't translated yet
#[derive(Parser, Debug)]
pub(crate) struct Api {
    /*
    - api:
          args:
            - FORMAT:
                short: f
                multiple_values: false
                multiple_occurrences: false
                about: Sets the output format
                possible_values: [json, yaml]
                takes_value: true
            - CONFIG:
                short: c
                long: config
                value_name: FILE
                about: Sets a custom config file
                takes_value: true
          subcommands:
            - version:
                about: Get tremor version
            - target:
                about: Target one or many tremor server instances
                subcommands:
                  - list:
                      about: List registered targets
                  - create:
                      about: Create a new API target
                      args:
                        - TARGET_ID:
                            about: The unique target id for the targetted tremor servers
                            required: true
                            takes_value: true
                        - SOURCE:
                            about: JSON or YAML file request body
                            required: true
                            takes_value: true
                  - delete:
                      about: Delete an existing API target
                      args:
                        - TARGET_ID:
                            about: The unique target id for the targetted tremor servers
                            required: true
                            takes_value: true
            - binding:
                about: Query/update binding specification repository
                subcommands:
                  - list:
                      about: List registered binding specifications
                  - fetch:
                      about: Fetch a binding by artefact id
                      args:
                        - ARTEFACT_ID:
                            about: The unique artefact id for the binding specification
                            required: true
                            takes_value: true
                  - delete:
                      about: Delete a binding by artefact id
                      args:
                        - ARTEFACT_ID:
                            about: The unique artefact id for the binding specification
                            required: true
                            takes_value: true
                  - create:
                      about: Create and register a binding specification
                      args:
                        - SOURCE:
                            about: JSON or YAML file request body
                            takes_value: true
                            required: true
                  - instance:
                      about: Fetch an binding instance by artefact id and instance id
                      args:
                        - ARTEFACT_ID:
                            about: The unique artefact id for the binding specification
                            required: true
                            takes_value: true
                        - INSTANCE_ID:
                            about: The unique instance id for the binding specification
                            required: true
                            takes_value: true
                  - activate:
                      about: Activate a binding by artefact id and servant instance id
                      args:
                        - ARTEFACT_ID:
                            about: The unique artefact id for the binding specification
                            required: true
                            takes_value: true
                        - INSTANCE_ID:
                            about: The unique instance id for the binding specification
                            required: true
                            takes_value: true
                        - SOURCE:
                            about: JSON -r YAML file request body
                            required: true
                            takes_value: true
                  - deactivate:
                      about: Activate a binding by artefact id and servant instance id
                      args:
                        - ARTEFACT_ID:
                            about: The unique artefact id for the binding specification
                            required: true
                            takes_value: true
                        - INSTANCE_ID:
                            about: The unique instance id for the binding specification
                            required: true
                            takes_value: true
            - pipeline:
                about: Query/update pipeline specification repository
                subcommands:
                  - list:
                      about: List registered pipeline specifications
                  - fetch:
                      about: Fetch a pipeline by artefact id
                      args:
                        - ARTEFACT_ID:
                            about: The unique artefact id for the pipeline specification
                            required: true
                            takes_value: true
                  - delete:
                      about: Delete a pipeline by artefact id
                      args:
                        - ARTEFACT_ID:
                            about: The unique artefact id for the pipeline specification
                            required: true
                            takes_value: true
                  - create:
                      about: Create and register a pipeline specification
                      args:
                        - SOURCE:
                            about: JSON or YAML file request body
                            required: false
                            takes_value: true
                  - instance:
                      about: Fetch an pipeline instance by artefact id and instance id
                      args:
                        - ARTEFACT_ID:
                            about: The unique artefact id for the pipeline specification
                            required: true
                            takes_value: true
                        - INSTANCE_ID:
                            about: The unique instance id for the pipeline specification
                            required: true
                            takes_value: true
            - onramp:
                about: Query/update onramp specification repository
                subcommands:
                  - list:
                      about: List registered onramp specifications
                  - fetch:
                      about: Fetch an onramp by artefact id
                      args:
                        - ARTEFACT_ID:
                            about: The unique artefact id for the onramp specification
                            required: true
                            takes_value: true
                  - delete:
                      about: Delete an onramp by artefact id
                      args:
                        - ARTEFACT_ID:
                            about: The unique artefact id for the onramp specification
                            required: true
                            takes_value: true
                  - create:
                      about: Create and register an onramp specification
                      args:
                        - SOURCE:
                            about: JSON or YAML file request body
                            required: false
                            takes_value: true
                  - instance:
                      about: Fetch an onramp instance by artefact id and instance id
                      args:
                        - ARTEFACT_ID:
                            about: The unique artefact id for the onramp specification
                            required: true
                            takes_value: true
                        - INSTANCE_ID:
                            about: The unique instance id for the onramp specification
                            required: true
                            takes_value: true
            - offramp:
                about: Query/update offramp specification repository
                subcommands:
                  - list:
                      about: List registered offramp specifications
                  - fetch:
                      about: Fetch an offramp by artefact id
                      args:
                        - ARTEFACT_ID:
                            about: The unique artefact id for the offramp specification
                            required: true
                            takes_value: true
                  - delete:
                      about: Delete an offramp by artefact id
                      args:
                        - ARTEFACT_ID:
                            about: The unique artefact id for the offramp specification
                            required: true
                            takes_value: true
                  - create:
                      about: Create and register an offramp specification
                      args:
                        - SOURCE:
                            about: JSON or YAML file request body
                            required: false
                            takes_value: true
                  - instance:
                      about: Fetch an offramp instance by artefact id and instance id
                      args:
                        - ARTEFACT_ID:
                            about: The unique artefact id for the offramp specification
                            required: true
                            takes_value: true
                        - INSTANCE_ID:
                            about: The unique instance id for the offramp specification
                            required: true
                            takes_value: true
        */
}
