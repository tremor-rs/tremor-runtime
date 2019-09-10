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

use crate::errors::Error;
use crate::onramp::prelude::*;
use crate::utils;
use actix_router::ResourceDef;
use actix_web::{
    dev::Payload, web, web::Data, App, FromRequest, HttpRequest, HttpResponse, HttpServer,
};
use futures::future::{result, Future};
use halfbrown::HashMap;
use http::{Method, StatusCode};
use serde_yaml::Value;
use simd_json::json;

#[derive(Debug, Clone, Deserialize, Default)]
pub struct Config {
    /// host to listen to, defaults to "0.0.0.0"
    #[serde(default = "dflt_host")]
    pub host: String,
    /// port to listen to, defaults to 8000
    #[serde(default = "dflt_port")]
    pub port: u32,
    pub resources: Vec<RestConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RestConfig {
    path: String,
    allow: Vec<ResourceConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ResourceConfig {
    method: HttpMethod,
    params: Option<Vec<String>>,
    status_code: usize,
}

fn dflt_host() -> String {
    String::from("0.0.0.0")
}

fn dflt_port() -> u32 {
    8000
}

#[derive(Clone, Debug)]
pub struct Rest {
    pub config: Config,
}

impl OnrampImpl for Rest {
    fn from_config(config: &Option<Value>) -> Result<Box<dyn Onramp>> {
        if let Some(config) = config {
            let config: Config = serde_yaml::from_value(config.clone())?;
            Ok(Box::new(Rest { config }))
        } else {
            Err("Missing config for REST onramp".into())
        }
    }
}

impl Onramp for Rest {
    fn start(&mut self, codec: String, preprocessors: Vec<String>) -> Result<OnrampAddr> {
        let config = self.config.clone();
        let (tx, rx) = bounded(0);
        thread::Builder::new()
            .name(format!("onramp-rest-{}", "???"))
            .spawn(|| onramp_loop(rx, config, preprocessors, codec))?;

        Ok(tx)
    }

    fn default_codec(&self) -> &str {
        "json"
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub enum HttpMethod {
    //    GET,
    POST,
    PUT,
    PATCH,
    DELETE,
    //    HEAD,
}

#[derive(Clone)]
struct OnrampState {
    tx: Option<Sender<Response>>,
    config: Config,
    preprocessors: Vec<Box<dyn Preprocessor>>,
}

impl Default for OnrampState {
    fn default() -> OnrampState {
        OnrampState {
            tx: None,
            config: Config::default(),
            preprocessors: vec![],
        }
    }
}

impl actix_web::error::ResponseError for Error {}

impl FromRequest for OnrampState {
    type Config = Self;
    type Error = Error;
    type Future = Result<Self>;

    fn from_request(req: &HttpRequest, _payload: &mut Payload) -> Result<Self> {
        let state = req.get_app_data::<OnrampState>();
        if let Some(st) = state {
            Ok(st.get_ref().to_owned())
        } else {
            Err("app cannot register state".into())
        }
    }
}
fn handler(
    sp: (Data<OnrampState>, web::Bytes, HttpRequest),
) -> Box<dyn Future<Item = HttpResponse, Error = Error>> {
    let state = sp.0;
    let payload = sp.1;
    let req = sp.2;
    let tx = state.tx.clone();
    let tx = match tx {
        Some(tx) => tx,
        None => unreachable!(),
    };

    // apply preprocessors
    let data = payload.to_vec();

    let preprocessors: Preprocessors = state.preprocessors.clone(); // PERF find a way to avoid clone
    for mut pp in preprocessors {
        match pp.process(utils::nanotime(), &data) {
            Ok(r) => {
                for data in r {
                    let data = std::str::from_utf8(&data).expect("invalid utf8 content");
                    let data = data.trim(); // PERF trim in place
                    if data.is_empty() {
                        continue;
                    }
                    let pp = path_params(state.config.resources.clone(), req.match_info().path());
                    let response = Response {
                        path: req.path().to_string(),
                        actual_path: pp.0,
                        query_params: req.query_string().to_owned(),
                        path_params: pp.1,
                        headers: header(req.headers()),
                        body: data.to_string(), // FIXME apply codecs to body content?
                        method: req.method().as_str().to_owned(),
                    };

                    let _ = tx.send(response);
                }
            }
            Err(e) => {
                return Box::new(futures::future::err(e));
            }
        };
    }

    let status = StatusCode::from_u16(match *req.method() {
        Method::POST => 201u16,
        Method::DELETE => 200u16,
        _ => 204u16,
    })
    .expect("bad status code");
    Box::new(result(Ok(HttpResponse::build(status).body("".to_string()))))
}

fn header(headers: &actix_web::http::header::HeaderMap) -> HashMap<String, String> {
    let mut hm = HashMap::new();

    headers.iter().for_each(|(key, value)| {
        hm.insert(
            key.as_str().to_string(),
            value.to_str().expect("header isn't set").to_string(),
        );
    });

    hm
}

fn path_params(patterns: Vec<RestConfig>, path: &str) -> (String, HashMap<String, String>) {
    for pattern in patterns {
        let mut path = actix_web::dev::Path::new(path);
        if ResourceDef::new(&pattern.path).match_path(&mut path) {
            return (
                pattern.path.clone(),
                path.iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect(),
            );
        }
    }
    (String::default(), HashMap::default())
}

fn onramp_loop(
    rx: Receiver<OnrampMsg>,
    config: Config,
    preprocessors: Vec<String>,
    codec: String,
) -> Result<()> {
    let host = format!("{}:{}", config.host, config.port);
    let (dt, dr) = bounded::<Response>(1);
    let preprocessors = make_preprocessors(&preprocessors)?;
    let _ = thread::Builder::new()
        .name(format!("onramp-rest-{}", "???"))
        .spawn(move || {
            let data = Data::new(OnrampState {
                tx: Some(dt),
                config,
                preprocessors,
            });
            let s = HttpServer::new(move || {
                App::new()
                    .register_data(data.clone())
                    .service(web::resource("/*").to(handler))
            })
            .bind(host);

            if let Ok(server) = s {
                let _ = server.run();
            } else {
                return error!("Cannot bind to host");
            }
        })?;
    let mut pipelines: Vec<(TremorURL, PipelineAddr)> = Vec::new();
    let mut codec = codec::lookup(&codec).expect("");
    let mut preprocessors = Vec::<Box<dyn preprocessor::Preprocessor>>::new();

    loop {
        if pipelines.is_empty() {
            match rx.recv() {
                Ok(OnrampMsg::Connect(ps)) => pipelines.append(&mut ps.clone()),
                Ok(OnrampMsg::Disconnect { tx, .. }) => {
                    let _ = tx.send(true);
                    return Ok(());
                }
                Err(e) => error!("{}", e),
            };
            continue;
        } else {
            match dr.try_recv() {
                Ok(data) => {
                    let data = json!(data).to_string().into_bytes();
                    send_event(&pipelines, &mut preprocessors, &mut codec, 0, data);
                    continue;
                }
                Err(TryRecvError::Empty) => (),
                Err(TryRecvError::Disconnected) => {
                    return Ok(());
                }
            }
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Response {
    path: String,
    query_params: String,
    actual_path: String,
    path_params: HashMap<String, String>,
    headers: HashMap<String, String>,
    body: String,
    method: String,
}
