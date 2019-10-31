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
    pub port: u16,
    pub resources: Vec<EndpointConfig>,
}

impl ConfigImpl for Config {}

#[derive(Debug, Clone, Deserialize)]
pub struct EndpointConfig {
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

fn dflt_port() -> u16 {
    8000
}

#[derive(Clone, Debug)]
pub struct Rest {
    pub config: Config,
}

impl onramp::Impl for Rest {
    fn from_config(config: &Option<Value>) -> Result<Box<dyn Onramp>> {
        if let Some(config) = config {
            let config: Config = Config::new(config)?;
            Ok(Box::new(Self { config }))
        } else {
            Err("Missing config for REST onramp".into())
        }
    }
}

impl Onramp for Rest {
    fn start(&mut self, codec: &str, preprocessors: &[String]) -> Result<onramp::Addr> {
        let config = self.config.clone();
        let (tx, rx) = bounded(0);
        let codec = codec::lookup(&codec)?;
        // rest is special
        let preprocessors = preprocessors.to_vec();
        thread::Builder::new()
            .name(format!("onramp-rest-{}", "???"))
            .spawn(move || onramp_loop(&rx, config, preprocessors, codec))?;

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
    tx: Sender<RestOnrampMessage>,
    config: Config,
}

impl Default for OnrampState {
    fn default() -> Self {
        let (tx, _) = bounded(1);
        Self {
            tx,
            config: Config::default(),
        }
    }
}
impl actix_web::error::ResponseError for Error {}

impl FromRequest for OnrampState {
    type Config = Self;
    type Error = Error;
    type Future = Result<Self>;

    fn from_request(req: &HttpRequest, _payload: &mut Payload) -> Result<Self> {
        let state = req.get_app_data::<Self>();
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
    // apply preprocessors
    let body = payload.to_vec();

    let pp = path_params(state.config.resources.clone(), req.match_info().path());
    let response = Response {
        path: req.path().to_string(),
        actual_path: pp.0,
        query_params: req.query_string().to_owned(),
        path_params: pp.1,
        headers: header(req.headers()),
        body,
        method: req.method().as_str().to_owned(),
    };
    // TODO cache parts of this and update host only on new request
    let origin_uri = tremor_pipeline::EventOriginUri {
        scheme: "tremor-rest".to_string(),
        host: req
            .connection_info()
            .remote()
            .unwrap_or("tremor-rest-client.remote")
            .to_string(),
        port: None,
        // TODO add server port here (like for tcp onramp) -- can be done via OnrampState
        path: vec![String::default()],
    };

    if let Err(_e) = tx.send((origin_uri, response)) {
        Box::new(result(Err("Failed to send to pipeline".into())))
    } else {
        let status = StatusCode::from_u16(match *req.method() {
            Method::POST => 201_u16,
            Method::DELETE => 200_u16,
            _ => 204_u16,
        })
        .unwrap_or_default();
        Box::new(result(Ok(HttpResponse::build(status).body("".to_string()))))
    }
}

fn header(headers: &actix_web::http::header::HeaderMap) -> HashMap<String, String> {
    headers
        .iter()
        .filter_map(|(key, value)| {
            Some((key.as_str().to_string(), value.to_str().ok()?.to_string()))
        })
        .collect()
}

fn path_params(patterns: Vec<EndpointConfig>, path: &str) -> (String, HashMap<String, String>) {
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

// We got to allow this because of the way that the onramp works
// by creating new instances during runtime.
#[allow(clippy::needless_pass_by_value)]
fn onramp_loop(
    rx: &Receiver<onramp::Msg>,
    config: Config,
    preprocessors: Vec<String>,
    mut codec: std::boxed::Box<dyn codec::Codec>,
) -> Result<()> {
    let host = format!("{}:{}", config.host, config.port);
    let (tx, dr) = bounded::<RestOnrampMessage>(1);
    thread::Builder::new()
        .name(format!("onramp-rest-{}", "???"))
        .spawn(move || {
            let data = Data::new(OnrampState { tx, config });
            let s = HttpServer::new(move || {
                App::new()
                    .register_data(data.clone())
                    .service(web::resource("/*").to(handler))
            })
            .bind(host);

            if let Err(err) = s.and_then(HttpServer::run) {
                return error!("Cannot run server: {}", err);
            }
        })?;
    let mut pipelines: Vec<(TremorURL, PipelineAddr)> = Vec::new();
    let mut preprocessors = make_preprocessors(&preprocessors)?;

    loop {
        if pipelines.is_empty() {
            match rx.recv() {
                Ok(onramp::Msg::Connect(ps)) => pipelines.append(&mut ps.clone()),
                Ok(onramp::Msg::Disconnect { tx, .. }) => {
                    tx.send(true)?;
                    return Ok(());
                }
                Err(e) => error!("{}", e),
            };
            continue;
        } else {
            match dr.try_recv() {
                Ok((origin_uri, data)) => {
                    let data = json!(data).encode().into_bytes();
                    let mut ingest_ns = nanotime();
                    send_event(
                        &pipelines,
                        &mut preprocessors,
                        &mut codec,
                        &mut ingest_ns,
                        &origin_uri,
                        0,
                        data,
                    );
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
    body: Vec<u8>,
    method: String,
}

type RestOnrampMessage = (tremor_pipeline::EventOriginUri, Response);
