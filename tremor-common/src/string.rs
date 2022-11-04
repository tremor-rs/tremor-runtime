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

use crate::errors::Error;
use std::{slice::SliceIndex, str};

/// Retrieves a substring from a slice of u8s
///
/// # Examples
///
/// ```
///  use tremor_common::string::substr;
///  let data = b"foo:1620649445.3351967|h";
///  let a = substr(data, 0..3).unwrap();
///
///  assert_eq!(a, "foo");
/// ```
///
/// # Errors
///
/// `Error::SubstringOutOfBounds` if the index is out of bounds
///
pub fn substr<I: SliceIndex<[u8], Output = [u8]>>(data: &[u8], r: I) -> Result<&str, Error> {
    let raw = data.get(r).ok_or(Error::SubstringOutOfBounds)?;
    let s = str::from_utf8(raw)?;
    Ok(s)
}

mod test {
    #![allow(clippy::unwrap_used)]

    #[test]
    fn test_subslice() {
        let a = b"012345";

        assert_eq!(super::substr(a, 1..).unwrap(), "12345");
        assert_eq!(super::substr(a, ..4).unwrap(), "0123");
        assert_eq!(super::substr(a, 1..4).unwrap(), "123");
        assert!(super::substr(a, 99..).is_err());
        assert!(super::substr(a, ..99).is_err());
    }
}
