// Copyright 2022, The Tremor Team
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

use std::time::{Duration, Instant};

use super::{setup_for_tls, ConnectorHarness};
use crate::connectors::impls::elastic;
use crate::connectors::impls::http::auth::Auth;
use crate::errors::{Error, Result};
use async_std::path::Path;
use elasticsearch::auth::{ClientCertificate, Credentials};
use elasticsearch::cert::{Certificate, CertificateValidation};
use elasticsearch::http::response::Response;
use elasticsearch::http::transport::{SingleNodeConnectionPool, TransportBuilder};
use elasticsearch::GetParts;
use elasticsearch::{http::transport::Transport, Elasticsearch};
use futures::TryFutureExt;
use http_types::convert::json;
use serial_test::serial;
use testcontainers::core::WaitFor;
use testcontainers::{clients, images::generic::GenericImage, RunnableImage};
use tremor_common::ports::IN;
use tremor_pipeline::{CbAction, Event, EventId};
use tremor_value::{literal, value::StaticValue};
use value_trait::{Mutable, Value, ValueAccess};

const ELASTICSEARCH_VERSION: &str = "8.4.1";

#[async_std::test]
#[serial(elastic)]
async fn connector_elastic() -> Result<()> {
    let _ = env_logger::try_init();

    let docker = clients::Cli::default();
    let port = super::free_port::find_free_tcp_port().await?;
    let image = RunnableImage::from(
        GenericImage::new("elasticsearch", ELASTICSEARCH_VERSION)
            .with_env_var("discovery.type", "single-node")
            .with_env_var("ES_JAVA_OPTS", "-Xms256m -Xmx256m")
            .with_env_var("xpack.security.enabled", "false")
            .with_env_var("xpack.security.http.ssl.enabled", "false"),
    )
    .with_mapped_port((port, 9200_u16));

    let container = docker.run(image);
    let port = container.get_host_port_ipv4(9200);

    // wait for the image to be reachable
    let elastic = Elasticsearch::new(Transport::single_node(
        format!("http://127.0.0.1:{port}").as_str(),
    )?);
    wait_for_es(&elastic).await?;

    let connector_config = literal!({
        "reconnect": {
            "retry": {
                "interval_ms": 1000,
                "max_retries": 10
            }
        },
        "config": {
            "nodes": [
                format!("http://127.0.0.1:{port}")
            ]
        }
    });
    let harness = ConnectorHarness::new(
        function_name!(),
        &elastic::Builder::default(),
        &connector_config,
    )
    .await?;
    let out = harness.out().expect("No pipe connected to port OUT");
    let err = harness.err().expect("No pipe connected to port ERR");
    let in_pipe = harness.get_pipe(IN).expect("No pipe connected to port IN");
    harness.start().await?;
    harness.wait_for_connected().await?;
    harness.consume_initial_sink_contraflow().await?;

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
    let err_events = err.get_events();
    assert!(err_events.is_empty(), "Received err msgs: {:?}", err_events);
    let event = out.get_event().await?;
    assert_eq!(
        &literal!({
            "elastic": {
                "_id": "123",
                "_index": "my_index",
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

    // upsert

    let data = literal!({
        "doc": {
            "field1": 13.4,
            "field2": "strong",
            "field3": [false, true],
            "field4": {
                "nested": false
            }
        },
        "doc_as_upsert": true,
    });
    let meta = literal!({
        "elastic": {
            "_index": "my_index",
            "_id": "1234",
            "action": "update",
            "raw_payload": true
        },
    });
    let event_not_batched = Event {
        id: EventId::default(),
        data: (data, meta).into(),
        transactional: false,
        ..Event::default()
    };
    harness.send_to_sink(event_not_batched, IN).await?;
    let err_events = err.get_events();
    assert!(err_events.is_empty(), "Received err msgs: {:?}", err_events);
    let event = out.get_event().await?;
    assert_eq!(
        &literal!({
            "elastic": {
                "_id": "1234",
                "_index": "my_index",
                "version": 1,
                "action": "update",
                "success": true
            },
        }),
        event.data.suffix().meta()
    );
    assert_eq!(
        &literal!({
            "update": {
                "_primary_term": 1,
                "_shards": {
                    "total": 2,
                    "successful": 1,
                    "failed": 0,
                },
                "_index": "my_index",
                "result": "created",
                "_id": "1234",
                "_seq_no": 1,
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
                "meta": {
                    "elastic": {
                        "_id": "versioned_id_001",
                        "version": 42,
                        "version_type": "external"
                    }
                }
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
    let meta = out_event1.data.suffix().meta().clone_static();

    assert_eq!(
        literal!({
            "elastic": {
                "_id": "versioned_id_001",
                "_index": "my_index",
                "version": 42,
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
                "_index": "my_index",
                "result": "created",
                "_seq_no": 2,
                "status": 201,
                "_version": 42
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

    // try an update with all da options
    let update_data = literal!({
        "snot": "badger"
    });
    let update_meta = literal!({
        "elastic": {
            "action": "update",
            "_id": "1234",
            "_index": "my_index",
            "if_seq_no": 1,
            "if_primary_term": 1,
            //"retry_on_conflict": 3
        }
    });
    let update_id = EventId::new(0, 0, 2, 2);
    let update_event = Event {
        id: update_id.clone(),
        is_batch: false,
        transactional: false,
        data: (update_data, update_meta).into(),
        ..Event::default()
    };
    harness.send_to_sink(update_event, IN).await?;
    let out_event = out.get_event().await?;
    let meta = out_event.data.suffix().meta();
    assert_eq!(
        literal!({
            "elastic": {
                "_id": "1234",
                "_index": "my_index",
                "version": 2,
                "action": "update",
                "success": true
            }
        }),
        meta
    );
    assert_eq!(
        literal!({
            "update": {
                "_primary_term": 1,
                "_shards": {
                    "total": 2,
                    "successful": 1,
                    "failed": 0
                },
                "_index": "my_index",
                "result": "updated",
                "_id": "1234",
                "_seq_no": 4,
                "status": 200,
                "_version": 2
            }
        }),
        out_event.data.suffix().value()
    );

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
    let mut err_event_meta = err_event.data.suffix().meta().clone_static();
    let err_msg = err_event_meta.remove("error")?;
    let err_msg_str = err_msg.unwrap();
    let err = err_msg_str.as_str().unwrap();
    assert!(
        err.contains("tcp connect error: Connection refused"),
        "{err} does not contain Connection refused"
    );

    assert_eq!(
        literal!({
            "elastic": {
                "success": false
            },
            "correlation": [true, false]
        }),
        err_event_meta
    );

    let (out_events, err_events) = harness.stop().await?;
    assert_eq!(out_events, vec![]);
    assert_eq!(err_events, vec![]);
    // will rm the container
    drop(container);

    Ok(())
}

#[async_std::test]
#[serial(elastic)]
async fn elastic_routing() -> Result<()> {
    let _ = env_logger::try_init();

    let docker = clients::Cli::default();
    let port = super::free_port::find_free_tcp_port().await?;
    let image = RunnableImage::from(
        GenericImage::new("elasticsearch", ELASTICSEARCH_VERSION)
            .with_env_var("discovery.type", "single-node")
            .with_env_var("ES_JAVA_OPTS", "-Xms256m -Xmx256m")
            .with_env_var("xpack.security.enabled", "false")
            .with_env_var("xpack.security.http.ssl.enabled", "false"),
    )
    .with_mapped_port((port, 9200_u16));

    let container = docker.run(image);
    let port = container.get_host_port_ipv4(9200);

    // wait for the image to be reachable
    let elastic = Elasticsearch::new(Transport::single_node(
        format!("http://127.0.0.1:{port}").as_str(),
    )?);
    wait_for_es(&elastic).await?;
    let index = "snot";
    elastic
        .indices()
        .create(elasticsearch::indices::IndicesCreateParts::Index(index))
        .body(json!({
            "settings": {
                "index": {
                    "number_of_shards": 5
                }
            }
        }))
        .send()
        .await?;

    let connector_config = literal!({
        "reconnect": {
            "retry": {
                "interval_ms": 1000,
                "max_retries": 10
            }
        },
        "config": {
            "index": index,
            "nodes": [
                format!("http://127.0.0.1:{port}")
            ]
        }
    });
    let harness = ConnectorHarness::new(
        function_name!(),
        &elastic::Builder::default(),
        &connector_config,
    )
    .await?;
    let out = harness.out().expect("No pipe connected to port OUT");
    harness.start().await?;
    harness.wait_for_connected().await?;
    harness.consume_initial_sink_contraflow().await?;

    // send a batched event
    let batched_data = literal!([{
            "data": {
                "value": {
                    "field1": 0.1,
                    "field2": "another_string",
                    "field3": [],
                },
                "meta": {
                    "elastic": {
                        "_id": "01",
                        "version": 42,
                        "version_type": "external",
                    }
                }
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
                        "_id": "02",
                    }
                }
            }
           },
        {
            "data": {
                "value": {},
                "meta": {
                    "elastic": {
                        "_id": "03",
                        "routing": "1"
                    },
                    "correlation": "snot",
                }
            }
        }
    ]);
    let batched_meta = literal!({
        "elastic": {
            "_index": index,
            "routing": "2"
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
    let meta = out_event1.data.suffix().meta();
    assert_eq!(
        literal!({
            "elastic": {
                "_id": "01",
                "_index": index,
                "version": 42,
                "action": "index",
                "success": true
            }
        }),
        meta
    );
    assert_eq!(
        literal!({
            "index": {
                "_primary_term": 1,
                "_shards": {
                    "total": 2,
                    "successful": 1,
                    "failed": 0
                },
                "_index": index,
                "result": "created",
                "_id": "01",
                "_seq_no": 0,
                "status": 201,
                "_version": 42
            }
        }),
        out_event1.data.suffix().value()
    );

    let out_event2 = out.get_event().await?;
    let meta = out_event2.data.suffix().meta();
    assert_eq!(
        literal!({
            "elastic": {
                "_id": "02",
                "_index": index,
                "version": 1,
                "action": "index",
                "success": true
            }
        }),
        meta
    );
    assert_eq!(
        literal!({
            "index": {
                "_primary_term": 1,
                "_shards": {
                    "total": 2,
                    "successful": 1,
                    "failed": 0
                },
                "_index": index,
                "result": "created",
                "_id": "02",
                "_seq_no": 1,
                "status": 201,
                "_version": 1
            }

        }),
        out_event2.data.suffix().value()
    );

    let out_event3 = out.get_event().await?;
    let meta = out_event3.data.suffix().meta();
    assert_eq!(
        literal!({
            "elastic": {
                "_id": "03",
                "_index": index,
                "version": 1,
                "action": "index",
                "success": true
            },
            "correlation": "snot"
        }),
        meta
    );
    assert_eq!(
        literal!({
            "index": {
                "_primary_term": 1,
                "_shards": {
                    "total": 2,
                    "successful": 1,
                    "failed": 0
                },
                "_index": index,
                "result": "created",
                "_id": "03",
                "_seq_no": 0,
                "status": 201,
                "_version": 1
            }
        }),
        out_event3.data.suffix().value()
    );

    let res = elastic.get(GetParts::IndexId(index, "01")).send().await?;
    assert_eq!(
        literal!({
            "_index": index,
            "_id": "01",
            "found": false
        }),
        &res.json::<simd_json::OwnedValue>().await?
    );
    let res = elastic
        .get(GetParts::IndexId(index, "01"))
        .routing("2")
        .send()
        .await?;
    assert_eq!(
        literal!({
            "_index": index,
            "_id": "01",
            "_version": 42,
            "_seq_no": 0,
            "_primary_term": 1,
            "_routing": "2",
            "found": true,
            "_source": {
                "field1": 0.1,
                "field2": "another_string",
                "field3": []
            }
        }),
        &res.json::<simd_json::OwnedValue>().await?
    );

    let res = elastic
        .get(GetParts::IndexId(index, "02"))
        .routing("2")
        .send()
        .await?;
    assert_eq!(
        literal!({
            "_index": index,
            "_id": "02",
            "_version": 1,
            "_seq_no": 1,
            "_primary_term": 1,
            "_routing": "2",
            "found": true,
            "_source": {
                "field3": 12,
                "field4": {
                    "nested": false,
                    "actually": "no"
                }
            }
        }),
        &res.json::<simd_json::OwnedValue>().await?
    );

    let res = elastic
        .get(GetParts::IndexId(index, "03"))
        .routing("2")
        .send()
        .await?;
    assert_eq!(
        literal!({
            "_index": index,
            "_id": "03",
            "found": false
        }),
        res.json::<simd_json::OwnedValue>().await?
    );

    let res = elastic
        .get(GetParts::IndexId(index, "03"))
        .routing("1")
        .send()
        .await?;
    assert_eq!(
        literal!({
            "_index": index,
            "_id": "03",
            "_version": 1,
            "_seq_no": 0,
            "_primary_term": 1,
            "_routing": "1",
            "found": true,
            "_source": {}
        }),
        &res.json::<simd_json::OwnedValue>().await?
    );

    let (out_events, err_events) = harness.stop().await?;
    assert_eq!(out_events, vec![]);
    assert_eq!(err_events, vec![]);
    // will rm the container
    drop(container);
    Ok(())
}

#[async_std::test]
#[serial(elastic)]
async fn auth_basic() -> Result<()> {
    let _ = env_logger::try_init();
    let docker = clients::Cli::default();
    let port = super::free_port::find_free_tcp_port().await?;
    let password = "snot";
    let image = RunnableImage::from(
        GenericImage::new("elasticsearch", ELASTICSEARCH_VERSION)
            .with_env_var("discovery.type", "single-node")
            .with_env_var("ES_JAVA_OPTS", "-Xms256m -Xmx256m")
            .with_env_var("ELASTIC_PASSWORD", password)
            .with_env_var("xpack.security.enabled", "true"),
    )
    .with_mapped_port((port, 9200_u16));

    let container = docker.run(image);
    let port = container.get_host_port_ipv4(9200);
    let conn_pool = SingleNodeConnectionPool::new(format!("http://127.0.0.1:{port}").parse()?);
    let transport = TransportBuilder::new(conn_pool).auth(Credentials::Basic(
        "elastic".to_string(),
        password.to_string(),
    ));
    let elastic = Elasticsearch::new(transport.build()?);
    wait_for_es(&elastic).await?;
    let index = "burngrain";

    let connector_config = literal!({
        "reconnect": {
            "retry": {
                "interval_ms": 1000,
                "max_retries": 10
            }
        },
        "config": {
            "auth": {
                "basic": {
                    "username": "elastic",
                    "password": password.to_string()
                }
            },
            "nodes": [
                format!("http://127.0.0.1:{port}")
            ],
            "index": index.to_string()
        }
    });
    let harness = ConnectorHarness::new(
        function_name!(),
        &elastic::Builder::default(),
        &connector_config,
    )
    .await?;
    harness.start().await?;
    harness.wait_for_connected().await?;
    harness.consume_initial_sink_contraflow().await?;

    send_one_event(&harness).await?;

    let (_out, err) = harness.stop().await?;
    assert!(err.is_empty());
    Ok(())
}

#[async_std::test]
#[serial(elastic)]
async fn auth_api_key() -> Result<()> {
    let _ = env_logger::try_init();
    let docker = clients::Cli::default();
    let port = super::free_port::find_free_tcp_port().await?;
    let password = "snot";
    let image = RunnableImage::from(
        GenericImage::new("elasticsearch", ELASTICSEARCH_VERSION)
            .with_env_var("discovery.type", "single-node")
            .with_env_var("ES_JAVA_OPTS", "-Xms256m -Xmx256m")
            .with_env_var("ELASTIC_PASSWORD", password)
            .with_env_var("xpack.security.enabled", "true")
            .with_env_var("xpack.security.authc.api_key.enabled", "true"),
    )
    .with_mapped_port((port, 9200_u16));

    let container = docker.run(image);
    let port = container.get_host_port_ipv4(9200);
    let conn_pool = SingleNodeConnectionPool::new(format!("http://127.0.0.1:{port}").parse()?);
    let transport = TransportBuilder::new(conn_pool).auth(Credentials::Basic(
        "elastic".to_string(),
        password.to_string(),
    ));
    let elastic = Elasticsearch::new(transport.build()?);
    wait_for_es(&elastic).await?;

    let index = "badger";
    let res = elastic
        .security()
        .create_api_key()
        .body(literal!({
            "name": "snot",
            "expiration": "1d"
        }))
        .send()
        .await?;
    let json = res.json::<StaticValue>().await?.into_value();
    let api_key_id = json.get_str("id").unwrap().to_string();
    let api_key = json.get_str("api_key").unwrap().to_string();
    let connector_config = literal!({
        "reconnect": {
            "retry": {
                "interval_ms": 1000,
                "max_retries": 10
            }
        },
        "config": {
            "auth": {
                "elastic_api_key": {
                    "id": api_key_id,
                    "api_key": api_key
                }
            },
            "nodes": [
                format!("http://127.0.0.1:{port}")
            ],
            "index": index.to_string()
        }
    });
    let harness = ConnectorHarness::new(
        function_name!(),
        &elastic::Builder::default(),
        &connector_config,
    )
    .await?;
    harness.start().await?;
    harness.wait_for_connected().await?;
    harness.consume_initial_sink_contraflow().await?;

    send_one_event(&harness).await?;

    let (_out, err) = harness.stop().await?;
    assert!(err.is_empty());
    Ok(())
}

#[async_std::test]
#[serial(elastic)]
async fn auth_client_cert() -> Result<()> {
    let _ = env_logger::try_init();
    setup_for_tls();

    let tests_dir = {
        let mut tmp = Path::new(file!()).to_path_buf();
        tmp.pop();
        tmp.pop();
        tmp.pop();
        tmp.pop();
        tmp.push("tests");
        tmp.canonicalize().await?
    };

    let docker = clients::Cli::default();
    let port = super::free_port::find_free_tcp_port().await?;
    let password = "snot";
    let image = RunnableImage::from(
        GenericImage::new("elasticsearch", ELASTICSEARCH_VERSION)
            .with_env_var("node.name", "snot")
            .with_env_var("discovery.type", "single-node")
            .with_env_var("ES_JAVA_OPTS", "-Xms256m -Xmx256m")
            .with_env_var("ELASTIC_PASSWORD", password)
            .with_env_var("xpack.security.enabled", "true")
            .with_env_var("xpack.security.http.ssl.client_authentication", "required")
            .with_env_var("xpack.security.http.ssl.enabled", "true")
            .with_env_var(
                "xpack.security.http.ssl.key",
                "/usr/share/elasticsearch/config/certificates/localhost.key",
            )
            .with_env_var(
                "xpack.security.http.ssl.certificate_authorities",
                "/usr/share/elasticsearch/config/certificates/localhost.cert",
            )
            .with_env_var(
                "xpack.security.http.ssl.certificate",
                "/usr/share/elasticsearch/config/certificates/localhost.cert",
            ),
    )
    .with_volume((
        tests_dir.display().to_string(),
        "/usr/share/elasticsearch/config/certificates",
    ))
    .with_mapped_port((port, 9200_u16));
    let mut cafile = tests_dir.clone();
    cafile.push("localhost.cert");
    let mut keyfile = tests_dir.clone();
    keyfile.push("localhost.key");

    let container = docker.run(image);
    let port = container.get_host_port_ipv4(9200);
    let conn_pool = SingleNodeConnectionPool::new(format!("https://localhost:{port}").parse()?);
    let ca = async_std::fs::read_to_string(&cafile).await?;
    let mut cert = async_std::fs::read(&cafile).await?;
    let mut key = async_std::fs::read(&keyfile).await?;
    key.append(&mut cert);
    let transport = TransportBuilder::new(conn_pool)
        .cert_validation(CertificateValidation::Full(Certificate::from_pem(
            ca.as_bytes(),
        )?))
        .auth(Credentials::Certificate(ClientCertificate::Pem(key)));
    let elastic = Elasticsearch::new(transport.build()?);
    if let Err(e) = wait_for_es(&elastic).await {
        let output = async_std::process::Command::new("docker")
            .args(&["logs", container.id()])
            .output()
            .await?;
        error!(
            "ELASTICSEARCH STDERR: {}",
            String::from_utf8_lossy(output.stdout.as_slice())
        );
        error!(
            "ELASTICSEARCH STDOUT: {}",
            String::from_utf8_lossy(output.stderr.as_slice())
        );
        return Err(e);
    }

    let index = "schmumbleglerp";

    let auth_header = Auth::Basic {
        username: "elastic".to_string(),
        password: "snot".to_string(),
    }
    .as_header_value()?
    .unwrap();

    let connector_config = literal!({
        "reconnect": {
            "retry": {
                "interval_ms": 1000,
                "max_retries": 10
            }
        },
        "config": {
            "tls": {
                "cafile": cafile.display().to_string(),
                "cert": cafile.display().to_string(),
                "key": keyfile.display().to_string()
            },
            "auth": {
                "basic": {
                    "username": "elastic",
                    "password": "snot"
                }
            },
            "nodes": [
                format!("https://localhost:{port}")
            ],
            "index": index.to_string(),
            // this test cannot test full PKI auth
            // it still needs another way of authenticating the user
            // as we don't have the required info in the certificates
            // and enabling the PKI realm in ES requires a license :(
            // but we can verify that if tls requires client certs, we do provide them
            "headers": {
                "Authorization": auth_header
            }
        }
    });
    let harness = ConnectorHarness::new(
        function_name!(),
        &elastic::Builder::default(),
        &connector_config,
    )
    .await?;
    let _out = harness.out().expect("No pipe connected to port OUT");
    harness.start().await?;
    harness.wait_for_connected().await?;
    harness.consume_initial_sink_contraflow().await?;

    send_one_event(&harness).await?;

    let (_out, err) = harness.stop().await?;
    assert!(err.is_empty());
    Ok(())
}

#[async_std::test]
#[serial(elastic)]
async fn elastic_https() -> Result<()> {
    let _ = env_logger::try_init();
    setup_for_tls();

    let tests_dir = {
        let mut tmp = Path::new(file!()).to_path_buf();
        tmp.pop();
        tmp.pop();
        tmp.pop();
        tmp.pop();
        tmp.push("tests");
        tmp.canonicalize().await?
    };

    let docker = clients::Cli::default();
    let port = super::free_port::find_free_tcp_port().await?;
    let password = "snot";
    let image = RunnableImage::from(
        GenericImage::new("elasticsearch", ELASTICSEARCH_VERSION)
            .with_env_var("node.name", "snot")
            .with_env_var("discovery.type", "single-node")
            .with_env_var("ES_JAVA_OPTS", "-Xms256m -Xmx256m")
            .with_env_var("ELASTIC_PASSWORD", password)
            .with_env_var("xpack.security.enabled", "true")
            .with_env_var("xpack.security.http.ssl.enabled", "true")
            .with_env_var(
                "xpack.security.http.ssl.key",
                "/usr/share/elasticsearch/config/certificates/localhost.key",
            )
            .with_env_var(
                "xpack.security.http.ssl.certificate_authorities",
                "/usr/share/elasticsearch/config/certificates/localhost.cert",
            )
            .with_env_var(
                "xpack.security.http.ssl.certificate",
                "/usr/share/elasticsearch/config/certificates/localhost.cert",
            )
            .with_wait_for(WaitFor::message_on_stdout("[YELLOW] to [GREEN]")),
    )
    .with_volume((
        tests_dir.display().to_string(),
        "/usr/share/elasticsearch/config/certificates",
    ))
    .with_mapped_port((port, 9200_u16));
    let mut cafile = tests_dir.clone();
    cafile.push("localhost.cert");

    let container = docker.run(image);
    let port = container.get_host_port_ipv4(9200);
    let conn_pool = SingleNodeConnectionPool::new(format!("https://localhost:{port}").parse()?);
    let ca = async_std::fs::read_to_string(&cafile).await?;
    let transport = TransportBuilder::new(conn_pool).cert_validation(CertificateValidation::Full(
        Certificate::from_pem(ca.as_bytes())?,
    ));
    let elastic = Elasticsearch::new(transport.build()?);
    if let Err(e) = wait_for_es(&elastic).await {
        let output = async_std::process::Command::new("docker")
            .args(&["logs", container.id()])
            .output()
            .await?;
        error!(
            "ELASTICSEARCH STDERR: {}",
            String::from_utf8_lossy(output.stdout.as_slice())
        );
        error!(
            "ELASTICSEARCH STDOUT: {}",
            String::from_utf8_lossy(output.stderr.as_slice())
        );
        return Err(e);
    }

    let index = "schmumbleglerp";

    let connector_config = literal!({
        "reconnect": {
            "retry": {
                "interval_ms": 1000,
                "max_retries": 10
            }
        },
        "config": {
            "tls": {
                "cafile": cafile.display().to_string(),
            },
            "auth": {
                "basic": {
                    "username": "elastic",
                    "password": "snot"
                }
            },
            "timeout": Duration::from_secs(10).as_nanos() as u64,
            "headers": {
                "x-custom": ["schmonglefoobs"]
            },
            "nodes": [
                format!("https://localhost:{port}")
            ],
            "index": index.to_string()
        }
    });
    let harness = ConnectorHarness::new(
        function_name!(),
        &elastic::Builder::default(),
        &connector_config,
    )
    .await?;
    let _out = harness.out().expect("No pipe connected to port OUT");
    harness.start().await?;
    harness.wait_for_connected().await?;
    harness.consume_initial_sink_contraflow().await?;

    send_one_event(&harness).await?;

    let (_out, err) = harness.stop().await?;
    assert!(err.is_empty());
    Ok(())
}

#[async_std::test]
async fn elastic_https_invalid_url() -> Result<()> {
    let connector_config = literal!({
        "reconnect": {
            "retry": {
                "interval_ms": 1000,
                "max_retries": 10
            }
        },
        "config": {
            "tls": true,
            "nodes": [
                format!("http://localhost:9200")
            ],
            "index": "schingelfrompf"
        }
    });
    assert!(ConnectorHarness::new(
        function_name!(),
        &elastic::Builder::default(),
        &connector_config,
    )
    .await
    .is_err());
    Ok(())
}

async fn wait_for_es(elastic: &Elasticsearch) -> Result<()> {
    // wait for the image to be reachable

    let wait_for = Duration::from_secs(60); // that shit takes a while
    let start = Instant::now();
    while let Err(e) = elastic
        .cluster()
        .health(elasticsearch::cluster::ClusterHealthParts::None)
        .send()
        .and_then(Response::json::<StaticValue>)
        .await
    {
        if start.elapsed() > wait_for {
            return Err(
                Error::from(e).chain_err(|| "Waiting for elasticsearch container timed out.")
            );
        }
        async_std::task::sleep(Duration::from_secs(1)).await;
    }
    Ok(())
}

async fn send_one_event(harness: &ConnectorHarness) -> Result<()> {
    let index = "fumbleschlonz";
    let data = literal!({"snot": "badger"});
    let meta = literal!({
        "elastic": {
            "_index": index.to_string(),
            "_id": "snot",
            "action": "index"
        },
        "correlation": 42
    });
    let event = Event {
        data: (data, meta).into(),
        ..Event::default()
    };
    harness.send_to_sink(event, IN).await?;
    let success_event = harness
        .out()
        .expect("NO pipeline connected to port OUT")
        .get_event()
        .await?;
    assert_eq!(
        &literal!({
            "index": {
                "_primary_term": 1,
                "_shards": {
                    "total": 2,
                    "successful": 1,
                    "failed": 0
                },
                "_index": index,
                "result": "created",
                "_id": "snot",
                "_seq_no": 0,
                "status": 201,
                "_version": 1
            }
        }),
        success_event.data.suffix().value()
    );
    assert_eq!(
        &literal!({
            "elastic": {
                "_id": "snot",
                "_index": index,
                "version": 1,
                "action": "index",
                "success": true
            },
            "correlation": 42
        }),
        success_event.data.suffix().meta()
    );
    Ok(())
}
