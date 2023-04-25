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
use base64::alphabet::STANDARD as STANDARD_ALPHABET;
use base64::engine::{DecodePaddingMode, GeneralPurpose, GeneralPurposeConfig};
pub use base64::Engine;

///  Our very own base64 engine, that produces base64 with padding, but accepts base64 with and without padding.
///  `BASE64` doesn't allow decoding without padding.
pub const BASE64: GeneralPurpose = GeneralPurpose::new(
    &STANDARD_ALPHABET,
    GeneralPurposeConfig::new()
        .with_encode_padding(true)
        .with_decode_padding_mode(DecodePaddingMode::Indifferent),
);

/// Encode a byte slice into a base64 string.
#[must_use]
pub fn encode<T>(data: T) -> String
where
    T: AsRef<[u8]>,
{
    BASE64.encode(data)
}

/// Decode a base64 string into a byte vector.
/// # Errors
/// Returns an error if the input is not valid base64.
pub fn decode<T>(data: T) -> Result<Vec<u8>, base64::DecodeError>
where
    T: AsRef<[u8]>,
{
    BASE64.decode(data)
}
