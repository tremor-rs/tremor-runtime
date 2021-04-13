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

use std::{
    collections::HashMap,
    fs::{self, File},
    io::Read,
};

use crate::errors::Result;
use reqwest::Client;
use tremor_value::Value;


pub(crate) async fn get_object<'event>(
    client: &Client,
    bucket_name: &str,
    object_name: &str,
) -> Result<Value<'event>> {
    let url = format!(
        "{}/b/{}/o/{}",
        "https://storage.googleapis.com/storage/v1", bucket_name, object_name
    );
    let body = client.get(url).send().await?.text().await?;
    Ok(Value::from(body))
}

pub(crate) async fn list_buckets<'event>(client: &Client, project_id: &str) -> Result<Value<'event>> {
    let url = format!("https://storage.googleapis.com/storage/v1/b?project={}", project_id);
    let body = client.get(url).send().await?.text().await?.into_bytes(); 
    to_value(body)

}

fn to_value<'event>(mut bytes: Vec<u8>) -> Result<Value<'event>> {
    Ok(tremor_value::parse_to_value(&mut bytes)?)
}

pub(crate) async fn list_objects<'event>(
    client: &Client,
    bucket_name: &str,
) -> Result<Value<'event>> {
    let url = format!(
        "{}/b/{}/o",
        "https://storage.googleapis.com/storage/v1",
        bucket_name.to_string()
    );
    let body = client.get(url).send().await?.text().await?;
    Ok(Value::from(body)) 
}

pub(crate) async fn add_object<'event>(
    client: &Client,
    bucket_name: &str,
    object_name: &str,
    _file_path: &str,
) -> Result<Value<'event>> {
    let url = format!(
        "https://storage.googleapis.com/upload/storage/v1/b/{}/o?uploadType=media&name={}",
        bucket_name, object_name
    );
    let buffer = get_file_as_byte_vec(_file_path);
    let body = client.post(url).body(buffer).send().await?.text().await?;
    // println!("File uploaded!");
    Ok(Value::from(body))
}

fn get_file_as_byte_vec(filename: &str) -> Vec<u8> {
    let mut f = File::open(&filename).expect("no file found");
    let metadata = fs::metadata(&filename).expect("unable to read metadata");
    let mut buffer = vec![0; metadata.len() as usize];
    f.read(&mut buffer).expect("buffer overflow");

    buffer
}

pub(crate) async fn delete_object<'event>(
    client: &Client,
    bucket_name: &str,
    object_name: &str,
) -> Result<Value<'event>> {
    let url = format!(
        "https://storage.googleapis.com/storage/v1/b/{}/o/{}",
        bucket_name, object_name
    );
    let body = client.delete(url).send().await?.text().await?;
    Ok(Value::from(body))
}

pub(crate) async fn download_object<'event>(
    client: &Client,
    bucket_name: &str,
    object_name: &str,
) -> Result<Value<'event>> {
    let url = format!(
        "{}/b/{}/o/{}?alt=media",
        "https://storage.googleapis.com/storage/v1", bucket_name, object_name
    );
    let response = client.get(url).send().await?;
    let body = response;
    let bytes = body.bytes().await?;
    Ok(Value::Bytes(bytes.to_vec().into()))
}

pub(crate) async fn create_bucket<'event>(
    client: &Client,
    project_id: &str,
    bucket_name: &str,
) -> Result<Value<'event>> {
    let url = format!(
        "https://storage.googleapis.com/storage/v1/b?project={}",
        project_id
    );
    let mut map = HashMap::new();
    map.insert("name", bucket_name);
    let body = client.post(url).json(&map).send().await?.text().await?;
    Ok(Value::from(body))
}

pub(crate) async fn delete_bucket<'event>(
    client: &Client,
    bucket_name: &str,
) -> Result<Value<'event>> {
    let url = format!(
        "https://storage.googleapis.com/storage/v1/b/{}",
        bucket_name
    );
    let body = client.delete(url).send().await?.text().await?;
    Ok(Value::from(body))
}
