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
use crate::codec::prelude::*;
use beef::Cow;

#[derive(Clone)]
pub struct Csv {}

impl Codec for Csv {
    fn name(&self) -> &str {
        "csv"
    }

    fn mime_types(&self) -> Vec<&'static str> {
        vec!["text/csv"]
    }

    fn decode<'input>(
        &mut self,
        data: &'input mut [u8],
        _ingest_ns: u64,
    ) -> Result<Option<Value<'input>>> {
        let mut reader = csv::ReaderBuilder::new()
            .has_headers(false)
            .from_reader(&*data); // the reborrow here is needed because std::io::Read is implemented only for &[u8], not &mut [u8]

        let record = match reader.records().next() {
            Some(Ok(x)) => Ok(x),
            Some(Err(e)) => Err(e),
            None => return Ok(None),
        }?;

        let mut fields = vec![];
        for field in record.iter() {
            fields.push(Value::String(Cow::from(field.to_string())));
        }

        Ok(Some(Value::Array(fields)))
    }

    fn encode(&self, data: &Value) -> Result<Vec<u8>> {
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

    #[test]
    fn test_can_decode_csv() {
        let mut codec = Csv {};
        let mut data = b"a,b,c,123".to_vec();
        let result = codec.decode(&mut data, 0);

        assert_eq!(Ok(Some(literal!(["a", "b", "c", "123"]))), result);
    }

    #[test]
    fn test_can_encode_csv() {
        let codec = Csv {};
        let data = literal!(["a", "b", "c", 123]);

        let result = codec.encode(&data).unwrap_or_default();

        assert_eq!(b"a,b,c,123".to_vec(), result);
    }
}
