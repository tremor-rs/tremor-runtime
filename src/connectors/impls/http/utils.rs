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

use http_types::Method;
use serde::de::Visitor;
use serde::{Deserialize, Deserializer};
use std::fmt;
use std::str::FromStr;

// We use surf for http clients
pub use surf::{
    Request as SurfRequest, RequestBuilder as SurfRequestBuilder, Response as SurfResponse,
};

// We use tide for http servers
pub use tide::{Request as TideRequest, Response as TideResponse, Result as TideResult};

struct MethodStrVisitor;

impl<'de> Visitor<'de> for MethodStrVisitor {
    type Value = SerdeMethod;
    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("an HTTP method string.")
    }

    fn visit_str<E>(self, v: &str) -> core::result::Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Method::from_str(v)
            .map(SerdeMethod)
            .map_err(|e| serde::de::Error::custom(e.to_string()))
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub struct SerdeMethod(pub(crate) Method);

impl<'de> Deserialize<'de> for SerdeMethod {
    fn deserialize<D>(deserializer: D) -> core::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(MethodStrVisitor)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::connectors::prelude::ConfigImpl;
    use crate::errors::Result;
    use tremor_value::literal;

    #[test]
    fn deserialize_method() -> Result<()> {
        let v = literal!({
          "url": "http://localhost:8080/",
          "method": "PATCH"
        });
        let config = super::super::meta::Config::new(&v)?;
        assert_eq!(Method::Patch, Method::from_str(&config.method)?);
        Ok(())
    }
}
