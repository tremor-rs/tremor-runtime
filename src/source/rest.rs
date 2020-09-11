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
use halfbrown::HashMap;
use simd_json::json;
use tide::http::Method;
use tide::{Body, Request, Response};

#[derive(Debug, Clone, Deserialize, Default)]
pub struct Config {
    /// host to listen to, defaults to "0.0.0.0"
    #[serde(default = "dflt_host")]
    pub host: String,
    /// port to listen to, defaults to 8000
    #[serde(default = "dflt_port")]
    pub port: u16,
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

#[derive(Serialize)]
pub struct TremorRestRequest {
    headers: HashMap<String, Vec<String>>,
    path: String,
    query_params: String,
    method: String,
    //body: Vec<u8>,
    body: String,
}

#[derive(Clone)]
struct ServerState {
    tx: Sender<SourceReply>,
    uid: u64,
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
                    .collect(),
            )
        })
        .collect();

    // TODO pass all of these as event meta (except body -- that goes as event data)
    // then can get rid of this struct
    let request = TremorRestRequest {
        headers,
        path: url.path().to_string(),
        // TODO introduce config param to pass this as a hashmap (useful when needed)
        // also document duplicate query key behavior in that case
        query_params: url.query().unwrap_or("").to_string(),
        method: req.method().to_string(),
        // TODO need to ultimately decode the body based on the request content-type
        // then this will be sent under SourceReply::Structured (along with event meta)
        //body: req.body_bytes().await?,
        body: req.body_string().await?,
    };
    let data = json!(request).encode().into_bytes();

    req.state()
        .tx
        .send(SourceReply::Data {
            origin_uri,
            data,
            stream: 0, // TODO some value here?
        })
        .await?;

    let status = match req.method() {
        Method::Post | Method::Put => 201,
        Method::Delete => 200,
        _ => 204,
    };

    Ok(Response::builder(status)
        .body(Body::from_string("".to_string()))
        .build())
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
        });
        // TODO add override for path and method from config (defaulting to
        // all paths and methods like below if not provided)
        server.at("/").all(handle_request);
        server.at("/*").all(handle_request);

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
