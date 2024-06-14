// Copyright 2021, The Tremor Team
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

use lazy_static::lazy_static;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;
pub use url::ParseError;

lazy_static! {
    // ALLOW: we know this regex is valid
    static ref URL_SCHEME_REGEX: Regex = Regex::new("^[A-Za-z-]+://").expect("Invalid Regex");
}

/// Default values for a URL
pub trait Defaults {
    /// Default scheme
    const SCHEME: &'static str;
    /// Default host
    const HOST: &'static str;
    /// Default port
    const PORT: u16;
}

/// Default HTTP
pub struct HttpDefaults;
impl Defaults for HttpDefaults {
    const HOST: &'static str = "localhost";
    const SCHEME: &'static str = "http";
    const PORT: u16 = 80;
}
/// Default HTTPS
pub struct HttpsDefaults;
impl Defaults for HttpsDefaults {
    const SCHEME: &'static str = "http";
    const HOST: &'static str = "localhost";
    const PORT: u16 = 443;
}

/// Endpoint URL
#[derive(Serialize)]
pub struct Url<D: Defaults = HttpDefaults> {
    url: url::Url,
    #[serde(skip)]
    _marker: PhantomData<D>,
}

// We have a custom deserializer since we want to not have it nested as\
// ```json
// {"url": "http..."}
// ```
impl<'de, Dflt: Defaults> Deserialize<'de> for Url<Dflt> {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let input = String::deserialize(deserializer)?;
        Url::parse(&input).map_err(serde::de::Error::custom)
    }
}

impl<D: Defaults, T> AsRef<T> for Url<D>
where
    url::Url: AsRef<T>,
{
    fn as_ref(&self) -> &T {
        self.url.as_ref()
    }
}

impl<D: Defaults, T> PartialEq<T> for Url<D>
where
    url::Url: PartialEq<T>,
{
    fn eq(&self, other: &T) -> bool {
        self.url == *other
    }
}

impl<D: Defaults> std::ops::Deref for Url<D> {
    type Target = url::Url;

    fn deref(&self) -> &Self::Target {
        &self.url
    }
}

impl<D: Defaults> std::ops::DerefMut for Url<D> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.url
    }
}

impl<D: Defaults> Clone for Url<D> {
    fn clone(&self) -> Self {
        Self {
            url: self.url.clone(),
            _marker: PhantomData,
        }
    }
}

impl<D: Defaults> std::fmt::Debug for Url<D> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Url").field("url", &self.url).finish()
    }
}

impl<D: Defaults> std::fmt::Display for Url<D> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.url)
    }
}

impl<D: Defaults> Default for Url<D> {
    fn default() -> Self {
        Self {
            url: url::Url::parse(&format!("{}://{}:{}", D::SCHEME, D::HOST, D::PORT))
                // ALLOW: this is a known safe url
                .expect("DEFAULT URL INVALID"),
            _marker: PhantomData,
        }
    }
}

impl<D: Defaults> Url<D> {
    /// Parse a URL
    /// # Errors
    /// if the URL is invalid
    pub fn parse(input: &str) -> Result<Self, ParseError> {
        let url = if URL_SCHEME_REGEX.is_match(input) {
            url::Url::parse(input)?
        } else {
            url::Url::parse(&format!("{}://{input}", D::SCHEME))?
        };
        Ok(Self {
            url,
            ..Self::default()
        })
    }

    /// fetches the port, if provided, or the default if not
    #[must_use]
    pub fn port_or_dflt(&self) -> u16 {
        self.url.port().unwrap_or(D::PORT)
    }

    /// fetches the host, if provided, or the default if not
    #[must_use]
    pub fn host_or_local(&self) -> &str {
        if let Some(host) = self.url.host_str() {
            // the url lib is shit in that it prints ipv6 addresses with the brackets (e.g. [::1])
            // but e.g. the socket handling libs want those addresses without, so we strip them here
            // See: https://github.com/servo/rust-url/issues/770
            if host.starts_with('[') {
                host.get(1..host.len() - 1).unwrap_or(D::HOST)
            } else {
                host
            }
        } else {
            D::HOST
        }
    }

    /// fetches the underlying raw URL
    #[must_use]
    pub fn url(&self) -> &url::Url {
        &self.url
    }
}

impl TryFrom<String> for Url {
    type Error = crate::errors::Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        match Url::parse(&value) {
            Ok(url) => Ok(url),
            Err(e) => Err(crate::Error::UrlParseError(e)),
        }
    }
}

impl TryFrom<&str> for Url {
    type Error = crate::errors::Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match Url::parse(value) {
            Ok(url) => Ok(url),
            Err(e) => Err(crate::Error::UrlParseError(e)),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn default() {
        Url::<HttpDefaults>::default();
        Url::<HttpsDefaults>::default();
    }

    use test_case::test_case;

    #[test_case("127.0.0.1", "http://127.0.0.1/"; "ensure scheme without port")]
    #[test_case("localhost:42", "http://localhost:42/"; "ensure scheme")]
    #[test_case("scheme://host:42/path?query=1&query=2#fragment", "scheme://host:42/path?query=1&query=2#fragment"; "all the url features")]
    fn serialize_deserialize(
        input: &str,
        expected: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut input = format!("\"{input}\""); // prepare for json compat
        let url: Url = unsafe { simd_json::from_str(&mut input)? };

        let serialized = url.to_string();
        assert_eq!(expected, &serialized);
        Ok(())
    }

    #[test]
    fn host_or_local_ipv6() {
        let url: Url<HttpDefaults> = Url::parse("[::1]:123").expect("valid url");
        assert_eq!(url.host_or_local(), "::1");
        let url: Url<HttpDefaults> = Url::parse("[::]:123").expect("valid url");
        assert_eq!(url.host_or_local(), "::");
    }

    #[test]
    fn invalid_host() {
        assert!(Url::<HttpDefaults>::parse("[").is_err());
    }

    #[test]
    fn try_from_string() {
        let url: Url = "http://localhost:42"
            .to_string()
            .try_into()
            .expect("valid url");
        assert_eq!(url.to_string(), "http://localhost:42/");

        let bad = "://";
        let url: Result<Url, _> = bad.to_string().try_into();
        assert!(url.is_err());
    }

    #[test]
    fn try_from_str() {
        let url: Url = "http://localhost:42".try_into().expect("valid url");
        assert_eq!(url.to_string(), "http://localhost:42/");

        let bad = ":\\";
        let url: Result<Url, _> = bad.to_string().try_into();
        assert!(url.is_err());
    }
}
