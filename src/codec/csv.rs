use crate::sink::prelude::*;

#[derive(Clone)]
pub struct Csv {}

impl Codec for Csv {
    fn name(&self) -> &str {
        "csv"
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
        match data {
            Value::Array(values) => {
                let mut fields = vec![];
                for value in values {
                    fields.push(format!("{}", value));
                }

                let mut result = vec![];
                let mut writer = csv::Writer::from_writer(&mut result);
                writer.write_record(&fields)?;
                writer.flush()?;
                drop(writer);

                Ok(result
                    .iter()
                    .take_while(|c| **c != b'\n' && **c != b'\r')
                    .copied()
                    .collect())
            }
            v => Err(crate::errors::ErrorKind::NotCSVSerializableValue(format!("{}", v)).into()),
        }
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

        let result = codec.encode(&data).unwrap();

        assert_eq!(b"a,b,c,123".to_vec(), result);
    }
}
