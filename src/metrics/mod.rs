use futures::future;
use hyper::rt::Future;
use hyper::{self, Body, Method, Request, Response, StatusCode};
use hyper::header;

use prometheus::{self, Encoder, TextEncoder};

type BoxFut = Box<Future<Item = Response<Body>, Error = hyper::Error> + Send>;

pub fn dispatch(req: Request<Body>) -> BoxFut {
    let mut res = Response::new(Body::empty());

    match (req.method(), req.uri().path()) {
        // Stats endpoint which provides prometheus style metrics
        (&Method::GET, "/metrics") => {
            let encoder = TextEncoder::new();
            let metric_familys = prometheus::gather();
            let mut buffer = vec![];
            encoder.encode(&metric_familys, &mut buffer).unwrap();
            res.headers_mut().insert("Access-Control-Allow-Origin", header::HeaderValue::from_static("*"));
            *res.body_mut() = Body::from(buffer);
        }
        _ => {
            *res.status_mut() = StatusCode::NOT_FOUND;
        }
    }
    Box::new(future::ok(res))
}
