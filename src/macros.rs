// Copyright 2018, Wayfair GmbH
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

macro_rules! opable {
    ( $e:ident, $( $i:ident ),* ) => {
        impl Opable for $e {
            fn on_timeout(&mut self) -> EventResult {
                match self {
                    $(
                        $e::$i(ref mut op) => op.on_timeout(),
                    )*
                }
            }
            fn on_event(&mut self, input: EventData) -> EventResult {
                match self {
                    $(
                        $e::$i(ref mut op) => op.on_event(input),
                    )*
                }
            }

            fn on_signal(&mut self, signal: &crate::pipeline::prelude::Signal) {
                match self {
                    $(
                        $e::$i(ref mut op) => op.on_signal(signal),
                    )*
                }
            }
            fn shutdown(&mut self) {
                match self {
                    $(
                        $e::$i(ref mut op) => op.shutdown(),
                    )*
                }
            }
            fn on_result(&mut self, result: EventReturn) -> EventReturn {
                match self {
                    $(
                        $e::$i(op) => op.on_result(result),
                    )*
                }
            }

            fn input_type(&self) -> ValueType {
                match self {
                    $(
                        $e::$i(op) => op.input_type(),
                    )*
                }
            }
            fn output_type(&self) -> ValueType {
                match self {
                    $(
                        $e::$i(op)  => op.output_type(),
                    )*

                }
            }
            fn input_vars(&self) -> HashSet<String> {
                match self {
                    $(
                        $e::$i(op) => op.input_vars(),
                    )*
                }
            }
            fn output_vars(&self) -> HashSet<String> {
                match self {
                    $(
                        $e::$i(op) => op.output_vars(),
                    )*
                }
            }
        }

    };
}

macro_rules! opable_types {
    ($in:expr, $out:expr) => {
        fn input_type(&self) -> ValueType {
            $in
        }
        fn output_type(&self) -> ValueType {
            $out
        }

    }
}

/// println_stderr and run_command_or_fail are copied from rdkafka-sys
macro_rules! println_stderr(
    ($($arg:tt)*) => { {
        let r = writeln!(&mut ::std::io::stderr(), $($arg)*);
        r.expect("failed printing to stderr");
    } }
);

macro_rules! ms {
    ($x:expr) => {
        1_000_000 * $x
    };
}

macro_rules! s {
    ($x:expr) => {
        1_000_000_000 * $x
    };
}

macro_rules! instance {
    // crate::metrics::INSTANCE is never muated after the initial setting
    // in main::run() so we can use this safely.
    () => {
        unsafe { crate::metrics::INSTANCE.to_string() }
    };
}
macro_rules! prom_int_gauge {
    ($name:expr, $desc:expr) => {
        register_int_gauge!(opts!($name, $desc)
            .namespace("tremor")
            .const_labels(hashmap! {"instance".to_string() => instance!()}))
        .unwrap()
    };
}

macro_rules! prom_gauge {
    ($name:expr, $desc:expr) => {
        register_gauge!(opts!($name, $desc)
            .namespace("tremor")
            .const_labels(hashmap! {"instance".to_string() => instance!()}))
        .unwrap()
    };
}

macro_rules! type_error {
    ($location:expr, $got:expr, $want:expr) => {
        Err(ErrorKind::TypeError($location.into(), $got, $want).into())
    };
}

macro_rules! error_result {
    ($event:expr, $error:expr) => {
        EventResult::Error(Box::new($event), Some($error.into()))
    };
}
macro_rules! next_id {
    ($id:expr, $event:expr) => {
        crate::pipeline::types::EventResult::NextID($id, Box::new($event))
    };
}

macro_rules! next {
    ($event:expr) => {
        EventResult::NextID(1, Box::new($event))
    };
}

macro_rules! return_result {
    ($event:expr) => {
        EventResult::Return(Box::new($event))
    };
}

macro_rules! ensure_type {
    ($event:expr, $location:expr, $type:expr) => {
        if !$event.is_type($type) {
            use crate::errors::ErrorKind;
            let t = $event.value.t();
            return error_result!($event, ErrorKind::TypeError($location.into(), t, $type));
        }
    };
}
