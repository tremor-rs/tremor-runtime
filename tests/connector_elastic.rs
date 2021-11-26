// Copyright 2021, The Tremor Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or imelied.
// See the License for the specific language governing permissions and
// limitations under the License.

mod connectors;

#[macro_use]
extern crate log;

use std::time::{Duration, Instant};

use async_std::stream::StreamExt;
use connectors::ConnectorHarness;
use elasticsearch::{http::transport::Transport, Elasticsearch};
use futures::TryFutureExt;
use signal_hook::consts::{SIGINT, SIGQUIT, SIGTERM};
use signal_hook_async_std::Signals;
use testcontainers::clients;
use testcontainers::images::generic::GenericImage;
use testcontainers::{Docker, RunArgs};
use tremor_common::url::ports::IN;
use tremor_pipeline::{CbAction, Event, EventId};
use tremor_runtime::errors::{Error, Result};
use tremor_value::{literal, value::StaticValue};
use value_trait::{Mutable, Value, ValueAccess};

const ELASTICSEARCH_VERSION: &'static str = "7.14.2";

#[async_std::test]
async fn connector_elastic() -> Result<()> {
    let _ = env_logger::try_init();

    let docker = clients::Cli::default();
    let image = GenericImage::new(format!("elasticsearch:{}", ELASTICSEARCH_VERSION))
        .with_env_var("discovery.type", "single-node");

    let container = docker.run_with_args(
        image,
        RunArgs::default().with_mapped_port((9200_u16, 9200_u16)),
    );
    // signal handling - stop and rm the container, even if we quit the test in the middle of everything
    let container_id = container.id().to_string();
    let mut signals = Signals::new(&[SIGTERM, SIGINT, SIGQUIT])?;
    let signal_handle = signals.handle();
    let signal_handler_task = async_std::task::spawn(async move {
        let signal_docker = clients::Cli::default();
        while let Some(_signal) = signals.next().await {
            signal_docker.stop(container_id.as_str());
            signal_docker.rm(container_id.as_str());
        }
    });
    let port = container.get_host_port(9200).unwrap_or(9200);

    // wait for the image to be reachable
    let elastic = Elasticsearch::new(Transport::single_node(
        format!("http://127.0.0.1:{}", port).as_str(),
    )?);
    let wait_for = Duration::from_secs(30); // that shit takes a while
    let start = Instant::now();
    // FIXME
    while let Err(e) = elastic
        .cluster()
        .health(elasticsearch::cluster::ClusterHealthParts::None)
        .send()
        .and_then(|r| r.json::<StaticValue>())
        .await
    {
        if start.elapsed() > wait_for {
            let mut logs = container.logs();
            let mut stderr = Vec::with_capacity(1024);
            let mut read = logs.stderr.read(stderr.as_mut_slice())?;
            while read > 0 {
                error!("ES ERR: {}", String::from_utf8_lossy(&stderr[0..read]));
                read = logs.stderr.read(stderr.as_mut_slice())?;
            }
            return Err(
                Error::from(e).chain_err(|| "Waiting for elasticsearch container timed out.")
            );
        }
        async_std::task::sleep(Duration::from_secs(1)).await;
    }

    let connector_yaml = format!(
        r#"
id: my_elastic
type: elastic
reconnect:
  custom:
    interval_ms: 1000
    max_retries: 10
config:
  nodes:
    - "http://127.0.0.1:{}"
"#,
        port
    );
    let harness = ConnectorHarness::new(connector_yaml).await?;
    let out = harness.out().expect("No pipe connected to port OUT");
    let err = harness.err().expect("No pipe connected to port ERR");
    let in_pipe = harness.get_pipe(IN).expect("No pipe connected to port IN");
    harness.start().await?;

    // CB Open is sent upon being connected
    let cf_event = in_pipe.get_contraflow().await?;
    assert_eq!(CbAction::Open, cf_event.cb);

    let data = literal!({
        "field1": 12.5,
        "field2": "string",
        "field3": [true, false],
        "field4": {
            "nested": true
        }
    });
    let meta = literal!({
        "elastic": {
            "_index": "my_index",
            "_id": "123",
            "action": "index"
        },
        "correlation": {
            "snot": ["badger", 42, false]
        }
    });
    let event_not_batched = Event {
        id: EventId::default(),
        data: (data, meta).into(),
        transactional: false,
        ..Event::default()
    };
    harness.send_to_sink(event_not_batched, IN).await?;
    let err_events = err.get_events()?;
    assert!(err_events.is_empty(), "Received err msgs: {:?}", err_events);
    let event = out.get_event().await?;
    assert_eq!(
        &literal!({
            "elastic": {
                "_id": "123",
                "_index": "my_index",
                "_type": "_doc",
                "version": 1,
                "action": "index",
                "success": true
            },
            "correlation": {
                "snot": ["badger", 42, false]
            }
        }),
        event.data.suffix().meta()
    );
    assert_eq!(
        &literal!({
            "index": {
                "_primary_term": 1,
                "_shards": {
                    "total": 2,
                    "successful": 1,
                    "failed": 0,
                },
                "_type": "_doc",
                "_index": "my_index",
                "result": "created",
                "_id": "123",
                "_seq_no": 0,
                "status": 201,
                "_version": 1
            }
        }),
        event.data.suffix().value()
    );

    let batched_data = literal!([{
            "data": {
                "value": {
                    "field1": 0.1,
                    "field2": "another_string",
                    "field3": [],
                },
                "meta": {}
            }
        },
        {
            "data": {
                "value": {
                    "field3": 12,
                    "field4": {
                        "nested": false,
                        "actually": "no"
                    }
                },
                "meta": {
                    "elastic": {
                        "action": "update",
                        "_id": "123",
                    }
                }
            }
           },
        {
            "data": {
                "value": {},
                "meta": {
                    "correlation": "snot"
                }
            }
        }
    ]);
    let batched_meta = literal!({
        "elastic": {
            "_index": "my_index",
            "_type": "_doc"
        }
    });
    let batched_id = EventId::new(0, 0, 1, 1);
    let event_batched = Event {
        id: batched_id.clone(),
        is_batch: true,
        transactional: true,
        data: (batched_data, batched_meta).into(),
        ..Event::default()
    };
    harness.send_to_sink(event_batched, IN).await?;
    let out_event1 = out.get_event().await?;
    let mut meta = out_event1.data.suffix().meta().clone_static();
    // remove _id
    assert!(meta
        .get_mut("elastic")
        .expect("no elastic in meta")
        .remove("_id")?
        .map(|v| v.is_str())
        .unwrap_or_default());
    assert_eq!(
        literal!({
            "elastic": {
                "_index": "my_index",
                "_type": "_doc",
                "version": 1,
                "action": "index",
                "success": true
            }
        }),
        meta
    );
    let mut data = out_event1.data.suffix().value().clone_static();
    // remove _id
    data.get_mut("index")
        .expect("no index in data")
        .remove("_id")?;
    assert_eq!(
        literal!({
            "index": {
                "_primary_term": 1,
                "_shards": {
                    "total": 2,
                    "successful": 1,
                    "failed": 0
                },
                "_type": "_doc",
                "_index": "my_index",
                "result": "created",
                "_seq_no": 1,
                "status": 201,
                "_version": 1
            }
        }),
        data
    );
    let err_event2 = err.get_event().await?;
    let meta = err_event2.data.suffix().meta();
    assert_eq!(
        literal!({
            "elastic": {
                "_id": "123",
                "_index": "my_index",
                "_type": "_doc",
                "action": "update",
                "success": false
            },
        }),
        meta
    );
    let data = err_event2.data.suffix().value();
    let data = data.get("update");
    let data = data.get("error");
    let reason = data.get_str("reason");
    assert_eq!(Some("failed to parse field [field3] of type [boolean] in document with id '123'. Preview of field's value: '12'"), reason);
    let out_event3 = out.get_event().await?;
    let mut meta = out_event3.data.suffix().meta().clone_static();
    // remove _id, as it is random
    assert!(meta
        .get_mut("elastic")
        .expect("No elastic in meta")
        .remove("_id")?
        .map(|s| s.is_str()) // at least check that it was a string
        .unwrap_or_default());

    assert_eq!(
        literal!({
            "elastic": {
                "success": true,
                "_index": "my_index",
                "_type": "_doc",
                "version": 1,
                "action": "index"
            },
            "correlation": "snot"
        }),
        meta
    );

    // a transactional event triggered a GD ACK
    let cf = in_pipe.get_contraflow().await?;
    assert_eq!(CbAction::Ack, cf.cb);
    assert_eq!(batched_id, cf.id);

    // check what happens when ES isnt reachable
    container.stop();

    let event = Event {
        id: EventId::new(0, 0, 2, 2),
        transactional: true,
        data: (
            literal!({}),
            literal!({
                "elastic": {
                    "_index": "my_index",
                    "action": "delete",
                    "_id": "123"
                },
                "correlation": [true, false]
            }),
        )
            .into(),
        ..Event::default()
    };
    harness.send_to_sink(event.clone(), IN).await?;
    let cf = in_pipe.get_contraflow().await?;
    assert_eq!(CbAction::Fail, cf.cb);
    let err_event = err.get_event().await?;
    assert_eq!(
        literal!({
            "elastic": {
                "success": false
            },
            "error": "error sending request for url (http://127.0.0.1:9200/my_index/_bulk): error trying to connect: tcp connect error: Connection refused (os error 61)",
            "correlation": [true, false]
        }),
        err_event.data.suffix().meta()
    );

    let (out_events, err_events) = harness.stop().await?;
    assert!(out_events.is_empty());
    assert!(err_events.is_empty());
    // cleanup
    signal_handle.close();
    signal_handler_task.cancel().await;
    // will rm the container
    drop(container);

    Ok(())
}
