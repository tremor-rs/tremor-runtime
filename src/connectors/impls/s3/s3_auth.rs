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

use crate::connectors::prelude::*;

use aws_types::region::Region;

use aws_sdk_s3 as s3;
use s3::Client as S3Client;
use s3::Endpoint;

pub async fn get_client(region: String, endpoint: Option<&String>) -> Result<S3Client> {
    // FIXME: Mathhias help D:
    // let region_provider = RegionProviderChain::default_provider().or_else("us-east-1");
    let region = Region::new(region);
    let config = aws_config::from_env().load().await;
    let mut config_builder = s3::config::Builder::from(&config).region(region);

    if let Some(endpoint) = endpoint {
        config_builder =
            config_builder.endpoint_resolver(Endpoint::immutable(endpoint.parse::<http::Uri>()?));
    }

    Ok(S3Client::from_conf(config_builder.build()))
}
