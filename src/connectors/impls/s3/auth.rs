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

use aws_config::meta::region::RegionProviderChain;
use aws_types::region::Region;

use aws_sdk_s3 as s3;
use s3::Client as S3Client;
use s3::Endpoint;

/// Get an S3 client for the given region and the optionally provided endpoint URL.
/// 
/// This client will use the default auth provider chain defined here: 
/// https://docs.rs/aws-config/latest/aws_config/default_provider/credentials/struct.DefaultCredentialsChain.html
/// 
/// It will try the following providers in order: (Based on aws-config 0.6.0)
/// 1. Environment variables
/// 2. Shared config (~/.aws/config, ~/.aws/credentials)
/// 3. Web Identity Tokens
/// 4. ECS (IAM Roles for Tasks) & General HTTP credentials
/// 5. EC2 IMDSv2
pub async fn get_client(region: Option<String>, endpoint: Option<&String>) -> Result<S3Client> {
    let region_provider = RegionProviderChain::first_try(region.map(Region::new)).or_default_provider();
    let region = region_provider.region().await;
    let config = aws_config::from_env().load().await;
    let mut config_builder = s3::config::Builder::from(&config).region(region);

    if let Some(endpoint) = endpoint {
        config_builder =
            config_builder.endpoint_resolver(Endpoint::immutable(endpoint.parse::<http::Uri>()?));
    }

    Ok(S3Client::from_conf(config_builder.build()))
}