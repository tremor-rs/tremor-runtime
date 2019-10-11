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

#![forbid(warnings)]
#![recursion_limit = "1024"]
#![cfg_attr(
    feature = "cargo-clippy",
    deny(
        clippy::all,
        clippy::result_unwrap_used,
        clippy::option_unwrap_used,
        clippy::unnecessary_unwrap,
        clippy::pedantic
    )
)]

use actix_web::{error, web, App, Error, HttpResponse, HttpServer};
use bytes::BytesMut;
use futures::{Future, Stream};
use std::sync::atomic::{AtomicUsize, Ordering};

//use std::sync::atomic::{AtomicU64, Ordering};

const MAX_SIZE: usize = 2_621_440; // max payload size is 256k

struct State {
    counter: AtomicUsize,
    size: AtomicUsize,
    _work_delay: u64,
    last_print: AtomicUsize,
}

fn index(
    data: web::Data<State>,
    payload: web::Payload,
) -> impl Future<Item = HttpResponse, Error = Error> {
    // payload is a stream of Bytes objects
    payload
        // `Future::from_err` acts like `?` in that it coerces the error type from
        // the future into the final error type
        .from_err()
        // `fold` will asynchronously read each chunk of the request body and
        // call supplied closure, then it resolves to result of closure
        .fold(BytesMut::new(), move |mut body, chunk| {
            // limit max size of in-memory payload
            if (body.len() + chunk.len()) > MAX_SIZE {
                Err(error::ErrorBadRequest("overflow"))
            } else {
                body.extend_from_slice(&chunk);
                Ok(body)
            }
        })
        // `Future::and_then` can be used to merge an asynchronous workflow with a
        // synchronous workflow
        .and_then(move |body| {
            // body is loaded, now we can deserialize serde-json
            let body: &[u8] = &body;
            let items = body.split(|v| *v == b'\n').count();
            let size = body.len();
            let c = data.counter.fetch_add(items, Ordering::Relaxed) + items;
            let s = data.size.fetch_add(size, Ordering::Relaxed) + size;
            let d = if c - data.last_print.load(Ordering::Relaxed) > 1000 {
                format!("{} / {} MB", c, s / 1024 / 1024)
            } else {
                String::new()
            };
            Ok(HttpResponse::Ok().body(d)) // <- send response
        })
}

fn main() -> std::io::Result<()> {
    let data = web::Data::new(State {
        counter: AtomicUsize::new(0),
        size: AtomicUsize::new(0),
        _work_delay: 0,
        last_print: AtomicUsize::new(0),
    });
    HttpServer::new(move || {
        App::new()
            .register_data(data.clone())
            .route("/", web::post().to_async(index))
    })
    .bind("127.0.0.1:8080")?
    .run()
}
