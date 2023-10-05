// Copyright 2020-2021, The Tremor Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! The `csv` codec provides marshalling support for the Comma Separated Volume format.
//!
//! The codec expects a single line of [RFC-4180](https://datatracker.ietf.org/doc/html/rfc4180) CSV format data.
//!
//! If there is more than a single line in the message, the lines after the first will be discarded unless
//! a `lines` preprocessor is used during deserialization.
//!
//! ## Example
//!
//! The following CSV line
//! ```csv
//! "some "" field",1234567,other_text,"2020-01-01 00:00:00"
//! ```
//!
//! Will get transalted the following equivalent tremor value:
//!
//! ```json
//! [
//!     "some \" field",
//!     "1234567",
//!     "other_text",
//!     "2020-01-01 00:00:00"
//! ]    
//! ```

use crate::prelude::*;
use beef::Cow;

#[derive(Clone)]
pub struct Csv {}

#[async_trait::async_trait]
impl Codec for Csv {
    fn name(&self) -> &str {
        "csv"
    }

    fn mime_types(&self) -> Vec<&'static str> {
        vec!["text/csv"]
    }

    async fn decode<'input>(
        &mut self,
        data: &'input mut [u8],
        _ingest_ns: u64,
        meta: Value<'input>,
    ) -> Result<Option<(Value<'input>, Value<'input>)>> {
        let mut reader = csv::ReaderBuilder::new()
            .has_headers(false)
            .from_reader(&*data); // the reborrow here is needed because std::io::Read is implemented only for &[u8], not &mut [u8]

        let record = match reader.records().next() {
            Some(Ok(x)) => Ok(x),
            Some(Err(e)) => Err(e),
            None => return Ok(None),
        }?;

        let mut fields = vec![];
        for field in &record {
            fields.push(Value::String(Cow::from(field.to_string())));
        }

        Ok(Some((Value::Array(fields), meta)))
    }

    async fn encode(&mut self, data: &Value, _meta: &Value) -> Result<Vec<u8>> {
        if let Some(values) = data.as_array() {
            let fields: Vec<String> = values.iter().map(ToString::to_string).collect();

            let mut result = vec![];
            let mut writer = csv::Writer::from_writer(&mut result);
            writer.write_record(&fields)?;
            writer.flush()?;
            drop(writer);

            while result.last() == Some(&b'\n') || result.last() == Some(&b'\r') {
                result.pop();
            }

            return Ok(result);
        }

        Err(
            crate::errors::ErrorKind::NotCSVSerializableValue(format!("{:?}", data.value_type()))
                .into(),
        )
    }

    fn boxed_clone(&self) -> Box<dyn Codec> {
        Box::new(self.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_can_decode_csv() -> Result<()> {
        let mut codec = Csv {};
        let mut data = b"a,b,c,123".to_vec();
        let result = codec
            .decode(&mut data, 0, Value::object())
            .await?
            .expect("no data");

        assert_eq!(literal!(["a", "b", "c", "123"]), result.0);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_can_encode_csv() -> Result<()> {
        let mut codec = Csv {};
        let data = literal!(["a", "b", "c", 123]);

        let result = codec.encode(&data, &Value::const_null()).await?;

        assert_eq!(b"a,b,c,123".to_vec(), result);
        Ok(())
    }
}
