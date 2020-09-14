// Copyright 2018-2020, Wayfair GmbH
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

// TODO add tests

use crate::source::prelude::*;
use async_channel::{Sender, TryRecvError};
use tide::{Body, Request, Response};
use tremor_script::Value;

#[derive(Debug, Clone, Deserialize, Default)]
pub struct Config {
    /// host to listen to, defaults to "0.0.0.0"
    #[serde(default = "dflt_host")]
    pub host: String,
    /// port to listen to, defaults to 8000
    #[serde(default = "dflt_port")]
    pub port: u16,
    /// whether to enable linked transport (return response based on pipeline output)
    // TODO remove and auto-infer this based on succesful binding for linked onramps
    pub link: Option<bool>,
}

// TODO possible to do this in source trait?
impl ConfigImpl for Config {}

fn dflt_host() -> String {
    String::from("0.0.0.0")
}

fn dflt_port() -> u16 {
    8000
}

pub struct Rest {
    pub config: Config,
    onramp_id: TremorURL,
}

impl onramp::Impl for Rest {
    fn from_config(id: &TremorURL, config: &Option<YamlValue>) -> Result<Box<dyn Onramp>> {
        if let Some(config) = config {
            let config: Config = Config::new(config)?;
            Ok(Box::new(Self {
                config,
                onramp_id: id.clone(),
            }))
        } else {
            Err("Missing config for REST onramp".into())
        }
    }
}

// TODO possible to do this in source trait?
pub struct Int {
    uid: u64,
    config: Config,
    listener: Option<Receiver<SourceReply>>,
    onramp_id: TremorURL,
}

impl std::fmt::Debug for Int {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "REST")
    }
}

impl Int {
    fn from_config(uid: u64, onramp_id: TremorURL, config: &Config) -> Result<Self> {
        let config = config.clone();

        Ok(Self {
            uid,
            config,
            listener: None,
            onramp_id,
        })
    }
}

#[derive(Clone)]
struct ServerState {
    tx: Sender<SourceReply>,
    uid: u64,
    link: bool,
}

async fn handle_request(mut req: Request<ServerState>) -> tide::Result<Response> {
    // TODO cache parts of this and update host only on new request
    let origin_uri = EventOriginUri {
        uid: req.state().uid,
        scheme: "tremor-rest".to_string(),
        host: req
            .host()
            .unwrap_or("tremor-rest-client-host.remote")
            .to_string(),
        port: None,
        // TODO add server port here (like for tcp onramp)
        path: vec![String::default()],
    };

    let url = req.url();
    let headers = req
        .header_names()
        .map(|name| {
            (
                name.to_string(),
                // a header name has the potential to take multiple values:
                // https://tools.ietf.org/html/rfc7230#section-3.2.2
                // tide does not seem to guarantee the order of values though --
                // look into it later
                req.header(name)
                    .iter()
                    .map(|value| value.as_str().to_string())
                    .collect::<Value>(),
            )
        })
        .collect::<Value>();

    // request metadata
    // TODO namespace these better?
    let mut meta = Value::object_with_capacity(4);
    meta.insert("request_method", req.method().to_string())?;
    meta.insert("request_path", url.path().to_string())?;
    // TODO introduce config param to pass this as a hashmap (useful when needed)
    // also document duplicate query key behavior in that case
    meta.insert("request_query", url.query().unwrap_or("").to_string())?;
    meta.insert("request_headers", headers)?;

    // TODO need to ultimately decode the body based on the request content-type
    //
    // alt strategy than the current one(s) here:
    // modify current codec and still pass data as SourceReply::Data
    // (adding capability there for meta). or can pass codec name along with the data
    let data = req.body_string().await?;
    //
    //let body = req.body_bytes().await?;
    //
    // need rental for this to work
    //let data = Value::from(std::str::from_utf8(&body)?);
    //
    // this does not allow us to pass meta
    //use crate::codec::Codec;
    //let codec = crate::codec::string::String {};
    //let data = codec.decode(body, 0)?.unwrap();
    //
    // works but is same logic as codecs, duplicated
    //let data = tremor_script::LineValue::try_new(vec![body], |data| {
    //    std::str::from_utf8(data[0].as_slice())
    //        .map(|v| tremor_script::ValueAndMeta::from_parts(Value::from(v), meta))
    //})
    //.map_err(|e| e.0)?;

    // TODO pass as function arg and turn on only when linking is on for the onramp
    //let wait_for_response = true;

    if req.state().link {
        let (response_tx, response_rx) = bounded(1);

        // TODO check how tide handles request timeouts here
        req.state()
            .tx
            .send(SourceReply::StructuredRequest {
                origin_uri,
                data: (data, meta).into(),
                //data,
                response_tx,
            })
            .await?;

        make_response(response_rx.recv().await?)
    } else {
        req.state()
            .tx
            .send(SourceReply::Structured {
                origin_uri,
                data: (data, meta).into(),
                //data,
            })
            .await?;

        Ok(Response::builder(202)
            .body(Body::from_string("".to_string()))
            .build())
    }
}

fn make_response(event: tremor_pipeline::Event) -> tide::Result<Response> {
    // TODO reject batched events and handle only single event here
    let (response_data, response_meta) = event.value_meta_iter().next().unwrap();

    // TODO need to ultimately encode the body based on the content-type header
    // set (if any). otherwise fall back to accept header, then to onramp codec,
    // setting response headers appropriately
    //
    // as json
    //let mut response_bytes = Vec::new();
    //response_data.write(&mut response_bytes)?;
    //
    // as string
    let response_bytes = if let Some(s) = response_data.as_str() {
        s.as_bytes().to_vec()
    } else {
        simd_json::to_vec(&response_data)?
    };

    let status = response_meta
        .get("response_status")
        .and_then(|s| s.as_u16())
        // TODO better default status?
        .unwrap_or(200);

    let mut response = Response::builder(status)
        .body(Body::from_bytes(response_bytes))
        .build();

    if let Some(headers) = response_meta.get("response_headers") {
        // TODO remove unwraps here
        for (name, values) in headers.as_object().unwrap() {
            for value in values.as_array().unwrap() {
                response.insert_header(name.as_ref(), value.as_str().unwrap());
            }
        }
    }

    Ok(response)
}

#[async_trait::async_trait()]
impl Source for Int {
    // TODO possible to do this in source trait?
    #[allow(unused_variables)]
    async fn pull_event(&mut self, id: u64) -> Result<SourceReply> {
        if let Some(listener) = self.listener.as_ref() {
            match listener.try_recv() {
                Ok(r) => Ok(r),
                Err(TryRecvError::Empty) => Ok(SourceReply::Empty(10)),
                Err(TryRecvError::Closed) => {
                    Ok(SourceReply::StateChange(SourceState::Disconnected))
                }
            }
        } else {
            Ok(SourceReply::StateChange(SourceState::Disconnected))
        }
    }
    async fn init(&mut self) -> Result<SourceState> {
        let (tx, rx) = bounded(crate::QSIZE);

        let mut server = tide::Server::with_state(ServerState {
            tx: tx.clone(),
            uid: self.uid,
            link: self.config.link.unwrap_or(false),
        });
        // TODO add override for path and method from config (defaulting to
        // all paths and methods like below if not provided)
        server.at("/").all(handle_request);
        server.at("/*").all(handle_request);
        // alt method without relying on server state
        //server.at("/*").all(|r| handle_request(r, self.uid, self.config.link.unwrap_or(false)));

        let addr = format!("{}:{}", self.config.host, self.config.port);

        task::spawn::<_, Result<()>>(async move {
            info!("[REST Onramp] Listening at {}", addr);
            if let Err(e) = server.listen(addr).await {
                error!("Error while listening from the rest server: {}", e)
            }
            warn!("[REST Onramp] Server stopped");

            // TODO better statechange here?
            tx.send(SourceReply::StateChange(SourceState::Disconnected))
                .await?;

            Ok(())
        });

        self.listener = Some(rx);

        // TODO ideally should happen only on successful server listen?
        Ok(SourceState::Connected)
    }
    fn id(&self) -> &TremorURL {
        &self.onramp_id
    }
}

#[async_trait::async_trait]
impl Onramp for Rest {
    async fn start(
        &mut self,
        onramp_uid: u64,
        codec: &str,
        preprocessors: &[String],
        metrics_reporter: RampReporter,
    ) -> Result<onramp::Addr> {
        let source = Int::from_config(onramp_uid, self.onramp_id.clone(), &self.config)?;
        SourceManager::start(onramp_uid, source, codec, preprocessors, metrics_reporter).await
    }

    fn default_codec(&self) -> &str {
        "json"
    }
}
