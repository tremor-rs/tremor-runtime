// Copyright 2018-2019, Wayfair GmbH
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

#![allow(deprecated)]

use error_chain::*;

#[cfg(test)]
impl PartialEq for Error {
    fn eq(&self, _other: &Error) -> bool {
        // This might be Ok since we try to compare Result in tets
        false
    }
}

error_chain! {

    foreign_links {
        ParseIntError(std::num::ParseIntError);
    }
    errors {
        ClonedError(t: String) {
            description("This is a cloned error we need to get rod of this")
                display("Cloned error: {}", t)
        }
        ParserError(s: String) {
            description("Partser error")
                display("Parser error: {}", s)
        }
        MutationTypeConflict(s: String) {
            description("Mutation Type Conflict")
                display("Mutation Type Conflict: {}", s)
        }
        BadPath(s: String) {
            description("BadPath")
                display("BadPath: {}", s)
        }
        RegexpError(s: String) {
            description("RegexpError")
                display("RegexpError: {}", s)
        }
        BadArrity(m: String, f: String, a: usize) {
            description("Bad arrity for function")
                display("Bad arrity for function {}::{}/{}", m, f, a)
        }
        MissingModule(m: String) {
            description("Call to undefined module")
                display("Call to undefined module {}", m)
        }
        MissingFunction(m: String, f: String) {
            description("Call to undefined function")
                display("Call to undefined function {}::{}", m, f)
        }
        BadType(m: String, f: String, a: usize) {
            description("Bad type passed to function")
                display("Bad type passed to function {}::{}/{}", m, f, a)
        }
        RuntimeError(m: String, f: String,  a: usize, c: String ) {
            description("Runtime error in function")
                display("Runtime error in function {}::{}/{}: {}", m, f, a, c)
        }

    }
}
