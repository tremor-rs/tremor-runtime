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

use std::collections::HashMap;

use crate::errors::Result;
use reqwest::Client;
use tremor_value::Value;

pub(crate) async fn get_object(
    client: &Client,
    bucket_name: &str,
    object_name: &str,
) -> Result<Value<'static>> {
    let url = format!(
        "{}/b/{}/o/{}",
        "https://storage.googleapis.com/storage/v1", bucket_name, object_name
    );
    let mut body = client.get(url).send().await?.text().await?.into_bytes();
    let body = tremor_value::parse_to_value(&mut body)?.into_static();
    Ok(body)
}

pub(crate) async fn list_buckets(client: &Client, project_id: &str) -> Result<Value<'static>> {
    let url = format!(
        "https://storage.googleapis.com/storage/v1/b?project={}",
        project_id
    );
    let mut body = client.get(url).send().await?.text().await?.into_bytes();
    let body = tremor_value::parse_to_value(&mut body)?.into_static();
    Ok(body)
}

pub(crate) async fn list_objects(client: &Client, bucket_name: &str) -> Result<Value<'static>> {
    let url = format!(
        "{}/b/{}/o",
        "https://storage.googleapis.com/storage/v1",
        bucket_name.to_string()
    );
    let mut body = client.get(url).send().await?.text().await?.into_bytes();
    let body = tremor_value::parse_to_value(&mut body)?.into_static();
    Ok(body)
}

pub(crate) async fn add_object_with_slice(
    client: &Client,
    bucket_name: &str,
    object_name: &str,
    content: Vec<u8>,
) -> Result<Value<'static>> {
    let url = format!(
        "https://storage.googleapis.com/upload/storage/v1/b/{}/o?uploadType=media&name={}",
        bucket_name, object_name
    );
    
    let mut body = client
        .post(url)
        .body(content)
        .send()
        .await?
        .text()
        .await?
        .into_bytes();
    let body = tremor_value::parse_to_value(&mut body)?.into_static();
    Ok(body)
}

pub(crate) async fn delete_object(
    client: &Client,
    bucket_name: &str,
    object_name: &str,
) -> Result<Value<'static>> {
    let url = format!(
        "https://storage.googleapis.com/storage/v1/b/{}/o/{}",
        bucket_name, object_name
    );
    let mut body = client.delete(url).send().await?.text().await?.into_bytes();
    let body = tremor_value::parse_to_value(&mut body)?.into_static();
    Ok(body)
}

pub(crate) async fn download_object<'event>(
    client: &Client,
    bucket_name: &str,
    object_name: &str,
) -> Result<Vec<u8>> {
    let url = format!(
        "{}/b/{}/o/{}?alt=media",
        "https://storage.googleapis.com/storage/v1", bucket_name, object_name
    );
    let response = client.get(url).send().await?;
    let body = response;
    let bytes = body.bytes().await?;
    Ok(bytes.to_vec())
}

pub(crate) async fn create_bucket(
    client: &Client,
    project_id: &str,
    bucket_name: &str,
) -> Result<Value<'static>> {
    let url = format!(
        "https://storage.googleapis.com/storage/v1/b?project={}",
        project_id
    );
    let mut map = HashMap::new();
    map.insert("name", bucket_name);
    let mut body = client
        .post(url)
        .json(&map)
        .send()
        .await?
        .text()
        .await?
        .into_bytes();
    let body = tremor_value::parse_to_value(&mut body)?.into_static();
    Ok(body)
}

pub(crate) async fn delete_bucket(client: &Client, bucket_name: &str) -> Result<Value<'static>> {
    let url = format!(
        "https://storage.googleapis.com/storage/v1/b/{}",
        bucket_name
    );
    let mut body = client.delete(url).send().await?.text().await?.into_bytes();
    let body = tremor_value::parse_to_value(&mut body)?.into_static();
    Ok(body)
}
