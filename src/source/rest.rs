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

use std::str::FromStr;

use crate::codec::Codec;
use crate::source::prelude::*;
use async_channel::unbounded;
use async_channel::{Sender, TryRecvError};
use halfbrown::HashMap;
use http_types::Mime;
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

struct RestSourceReply(Option<Sender<Response>>, SourceReply);

impl From<SourceReply> for RestSourceReply {
    fn from(reply: SourceReply) -> Self {
        Self(None, reply)
    }
}

// TODO possible to do this in source trait?
pub struct Int {
    uid: u64,
    config: Config,
    listener: Option<Receiver<RestSourceReply>>,
    onramp_id: TremorURL,
    // TODO better way to manage this?
    response_txes: HashMap<u64, Sender<Response>>,
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
            response_txes: HashMap::new(),
        })
    }
}

struct BoxedCodec(Box<dyn Codec>);

impl Clone for BoxedCodec {
    fn clone(&self) -> Self {
        Self(self.0.boxed_clone())
    }
}

impl From<Box<dyn Codec>> for BoxedCodec {
    fn from(bc: Box<dyn Codec>) -> Self {
        Self(bc)
    }
}

impl AsRef<Box<dyn Codec>> for BoxedCodec {
    fn as_ref(&self) -> &Box<dyn Codec> {
        &self.0
    }
}

#[derive(Clone)]
struct ServerState {
    tx: Sender<RestSourceReply>,
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

    // TODO: consider stuff like charset here?
    let ct: Option<Mime> = req.content_type();
    let codec_override = ct.map(|ct| ct.essence().to_string());

    // request metadata
    // TODO namespace these better?
    let mut meta = Value::object_with_capacity(4);
    meta.insert("request_method", req.method().to_string())?;
    meta.insert("request_url", req.url().as_str().to_string())?;
    // TODO introduce config param to pass url parts as a hashmap (useful when needed)
    // right now, users can parse these from tremor-script as needed
    //let url = req.url();
    // need to add username, port and fragment as well
    //meta.insert("request_scheme", url.scheme().to_string())?;
    //meta.insert("request_host", url.host_str().unwrap_or("").to_string())?;
    //meta.insert("request_path", url.path().to_string())?;
    // TODO introduce config param to pass this as a hashmap (useful when needed)
    // also document duplicate query key behavior in that case
    //meta.insert("request_query", url.query().unwrap_or("").to_string())?;
    meta.insert("request_headers", headers)?;
    meta.insert(
        "request_content_type",
        codec_override.clone().map_or(Value::null(), |s| s.into()),
    )?;

    let data = req.body_bytes().await?;
    if req.state().link {
        let (response_tx, response_rx) = unbounded();

        // TODO check how tide handles request timeouts here
        //
        // also figure out best way to bubble up errors (to the end user)
        // during processing of this data, that may occur before a valid
        // event is sent back from the pipeline.
        // eg: in case of invalid json input with json content-type/codec,
        // event processing fails at the codec decoding stage, even before
        // this event reaches pipeline, but the rest source request just
        // hangs (until it times out).
        req.state()
            .tx
            .send(RestSourceReply(
                Some(response_tx),
                SourceReply::Data {
                    origin_uri,
                    data,
                    meta: Some(meta),
                    codec_override,
                    stream: 0,
                },
            ))
            .await?;
        // TODO honor accept header
        Ok(response_rx.recv().await?)
    } else {
        req.state()
            .tx
            .send(
                SourceReply::Data {
                    origin_uri,
                    data,
                    meta: Some(meta),
                    codec_override,
                    stream: 0,
                }
                .into(),
            )
            .await?;

        // TODO set proper content-type
        Ok(Response::builder(202).body(Body::empty()).build())
    }
}

fn make_response(
    default_codec: &dyn Codec,
    codec_map: &HashMap<String, Box<dyn Codec>>,
    event: tremor_pipeline::Event,
) -> Result<Response> {
    // TODO reject batched events and handle only single event here
    let (response_data, response_meta) = event.value_meta_iter().next().unwrap();

    // TODO status should change if there's any errors here (eg: during
    // content-type encoding). maybe include the error string in the response too
    let status = response_meta
        .get("response_status")
        .and_then(|s| s.as_u16())
        .unwrap_or(200);

    let mut builder = Response::builder(status);
    // extract headers
    let mut header_content_type: Option<&str> = None;
    if let Some(headers) = response_meta
        .get("response_headers")
        .and_then(|hs| hs.as_object())
    {
        for (name, values) in headers {
            if let Some(header_values) = values.as_array() {
                if name.eq_ignore_ascii_case("content-type") {
                    // pick first value in case of multiple content-type headers
                    header_content_type = header_values[0].as_str();
                }
                for value in header_values {
                    if let Some(header_value) = value.as_str() {
                        builder = builder.header(name.as_ref(), header_value);
                    }
                }
            }
        }
    }

    // extract content_type
    // give content type from headers precedence
    // TODO: maybe ditch response_content_type meta stuff
    let maybe_content_type = match (
        header_content_type,
        response_meta
            .get("response_content_type")
            .and_then(|ct| ct.as_str()),
    ) {
        (None, None) => None,
        (None, Some(ct)) | (Some(ct), _) => Some(Mime::from_str(ct)?),
    };
    if let Some((mime, dynamic_codec)) = maybe_content_type
        .and_then(|mime| codec_map.get(mime.essence()).map(|c| (mime, c.as_ref())))
    {
        let mut body = Body::from_bytes(dynamic_codec.encode(response_data)?);
        body.set_mime(mime);
        builder = builder.body(body);
    } else {
        // fallback to default codec
        let mut body = Body::from_bytes(default_codec.encode(response_data)?);
        // set mime type for default codec
        // TODO: cache mime type for default codec
        if let Some(mime) = default_codec
            .mime_types()
            .drain(..)
            .find_map(|mstr| Mime::from_str(mstr).ok())
        {
            body.set_mime(mime);
        }
        builder = builder.body(body);
    }
    Ok(builder.build())
}

#[async_trait::async_trait()]
impl Source for Int {
    // TODO possible to do this in source trait?
    #[allow(unused_variables)]
    async fn pull_event(&mut self, id: u64) -> Result<SourceReply> {
        if let Some(listener) = self.listener.as_ref() {
            match listener.try_recv() {
                Ok(RestSourceReply(Some(response_tx), source_reply)) => {
                    // store a sender here to be able to send the response later
                    self.response_txes.insert(id, response_tx);
                    Ok(source_reply)
                }
                Ok(r) => Ok(r.1),
                Err(TryRecvError::Empty) => Ok(SourceReply::Empty(10)),
                Err(TryRecvError::Closed) => {
                    Ok(SourceReply::StateChange(SourceState::Disconnected))
                }
            }
        } else {
            Ok(SourceReply::StateChange(SourceState::Disconnected))
        }
    }

    async fn reply_event(
        &mut self,
        event: Event,
        codec: &dyn Codec,
        codec_map: &HashMap<String, Box<dyn Codec>>,
    ) -> Result<()> {
        if let Some(event_id) = event.id.get(self.uid) {
            if let Some(response_tx) = self.response_txes.get(&event_id) {
                let res = make_response(codec, codec_map, event)?;
                response_tx.send(res).await?
            }
        }
        Ok(())
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
            tx.send(SourceReply::StateChange(SourceState::Disconnected).into())
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
        codec_map: halfbrown::HashMap<String, String>,
        preprocessors: &[String],
        metrics_reporter: RampReporter,
    ) -> Result<onramp::Addr> {
        let source = Int::from_config(onramp_uid, self.onramp_id.clone(), &self.config)?;
        SourceManager::start(
            onramp_uid,
            source,
            codec,
            codec_map,
            preprocessors,
            metrics_reporter,
        )
        .await
    }

    fn default_codec(&self) -> &str {
        "json"
    }
}
