// Copyright 2020-2021, The Tremor Team
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

//! The `confluent-schema-registry` codec allows using the [Confluent Schema Registry](https://docs.confluent.io/platform/current/schema-registry/index.html)
//! as a source for avro decoding information.
//!
//! It can be used in combination with a kafka topic that encodes it's content in avro format and stores it's schema in the schema registry.
//!
//! For decoding avro data (from kafka or otherwise) that is manually encoded please use the [avro](./avro) codec.
//!
//! ## Configuration
//!
//!  - `url`: the `url` configuration is used to point to the root of the schema registry server
//!
//! ## Mappings
//!
//! The same as the [`avro` codec](./avro)

use crate::{
    avro::{avro_to_value, value_to_avro, SchemaResolver, SchemaWrapper},
    prelude::*,
};
use apache_avro::schema::Name;
use schema_registry_converter::async_impl::schema_registry::SrSettings;
use schema_registry_converter::{
    async_impl::easy_avro::{EasyAvroDecoder as AvroDecoder, EasyAvroEncoder as AvroEncoder},
    schema_registry_common::SubjectNameStrategy,
};
use tremor_common::url::{HttpDefaults, Url};

pub struct Csr {
    registry: Url<HttpDefaults>,
    settings: SrSettings,
    decoder: AvroDecoder,
    encoder: AvroEncoder,
}

#[allow(clippy::missing_fields_in_debug)]
impl std::fmt::Debug for Csr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Csr")
            .field("registry", &self.registry)
            .field("settings", &self.settings)
            .finish()
    }
}

impl Clone for Csr {
    fn clone(&self) -> Self {
        Self {
            registry: self.registry.clone(),
            settings: self.settings.clone(),
            decoder: AvroDecoder::new(self.settings.clone()),
            encoder: AvroEncoder::new(self.settings.clone()),
        }
    }
}

impl Csr {
    pub(crate) fn from_config(config: Option<&Value>) -> Result<Box<dyn Codec>> {
        let url = config
            .get_str("url")
            .map(ToString::to_string)
            .ok_or("Missing URL config for schema registry codec")?;

        let registry = Url::parse(&url)?;
        let settings = SrSettings::new(url.to_string());
        let decoder = AvroDecoder::new(settings.clone());
        let encoder = AvroEncoder::new(settings.clone());
        Ok(Box::new(Csr {
            registry,
            settings,
            decoder,
            encoder,
        }))
    }
}

struct RecordResolver<'a> {
    encoder: &'a AvroEncoder,
}

#[async_trait::async_trait]
impl SchemaResolver for RecordResolver<'_> {
    async fn by_name(&self, name: &Name) -> Option<SchemaWrapper> {
        self.encoder
            .get_schema_and_id(
                &name.name,
                SubjectNameStrategy::RecordNameStrategy(name.name.clone()),
            )
            .await
            .ok()
            .map(SchemaWrapper::Schema)
    }
}

#[async_trait::async_trait()]
impl Codec for Csr {
    fn name(&self) -> &str {
        "confluent-schema-registry"
    }

    async fn decode<'input>(
        &mut self,
        data: &'input mut [u8],
        _ingest_ns: u64,
        meta: Value<'input>,
    ) -> Result<Option<(Value<'input>, Value<'input>)>> {
        let r = self.decoder.decode(Some(data)).await?;
        let v = avro_to_value(r.value)?;
        Ok(Some((v, meta)))
    }

    #[must_use]
    async fn encode(&mut self, data: &Value, meta: &Value) -> Result<Vec<u8>> {
        let key = meta.try_get_str("key")?.ok_or("Missing key")?;
        let topic = meta
            .try_get_str("topic")?
            .ok_or("Missing topic")?
            .to_string();
        let strategy = SubjectNameStrategy::TopicNameStrategy(topic, true);

        let schema = self
            .encoder
            .get_schema_and_id(key, strategy.clone())
            .await?;

        // self.encoder.encode(values, subject_name_strategy)
        let avro_value = value_to_avro(
            data,
            &schema.parsed,
            &RecordResolver {
                encoder: &self.encoder,
            },
        )
        .await?;
        Ok(self
            .encoder
            .encode(vec![(key, avro_value)], strategy)
            .await?)
    }

    fn boxed_clone(&self) -> Box<dyn Codec> {
        Box::new(self.clone())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    // Test if the codec can be created from config
    #[test]
    fn test_codec_creation() {
        let config = literal!({"url":"http://localhost:8081"});
        let codec = Csr::from_config(Some(&config)).expect("invalid config");
        assert_eq!(codec.name(), "confluent-schema-registry");
    }

    #[test]
    fn invalid_config() {
        let config = literal!({});
        let codec = Csr::from_config(Some(&config));
        assert!(codec.is_err());
    }
    #[test]
    fn invalid_url() {
        let config = literal!({"url":"loc alhost:8081"});
        let codec = Csr::from_config(Some(&config));
        assert!(codec.is_err());
    }
}
