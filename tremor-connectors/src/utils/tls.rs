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

//! TLS utilities

use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::{ClientConfig, RootCertStore, ServerConfig};
use rustls_native_certs::load_native_certs;
use rustls_pemfile::{pkcs8_private_keys, rsa_private_keys, Item};
use std::io::{self, BufReader};
use std::{path::PathBuf, sync::Arc};
use tokio_rustls::TlsConnector;

lazy_static::lazy_static! {
    static ref SYSTEM_ROOT_CERTS: RootCertStore = {
        let mut roots = RootCertStore::empty();
        // ALLOW: this is expected to panic if we cannot load system certificates
        for cert in load_native_certs().expect("Unable to load system TLS certificates.") {
            roots
                .add(cert)
                // ALLOW: this is expected to panic if we cannot load system certificates
                .expect("Unable to add root TLS certificate to RootCertStore");
        }
        roots
    };
}

/// TLS Server Configuration
#[derive(Debug, Clone, Deserialize)]
pub struct TLSServerConfig {
    pub(crate) cert: PathBuf,
    pub(crate) key: PathBuf,
}

impl TLSServerConfig {
    /// creates a new server config
    #[must_use]
    pub fn new<P1, P2>(cert: P1, key: P2) -> Self
    where
        P1: Into<PathBuf>,
        P2: Into<PathBuf>,
    {
        TLSServerConfig {
            cert: cert.into(),
            key: key.into(),
        }
    }

    /// Create a new server config from the `TLSServerConfig`
    /// # Errors
    /// if the cert or key is invalid
    pub fn to_server_config(&self) -> Result<ServerConfig, Error> {
        let cert = &self.cert;
        let key = &self.key;
        let certs = load_certs(cert)?;

        let key = load_keys(key)?;

        let server_config = ServerConfig::builder()
            .with_no_client_auth()
            // set this server to use one cert together with the loaded private key
            .with_single_cert(certs, key)?;

        Ok(server_config)
    }
}

/// TLS Client Configuration
#[derive(Deserialize, Debug, Default, Clone)]
#[serde(deny_unknown_fields)]
pub struct TLSClientConfig {
    /// Path to the pem-encoded certificate file of the CA to use for verifying the servers certificate
    cafile: Option<PathBuf>,
    /// The DNS domain used to verify the server's certificate. If not provided the domain from the connection URL will be used.
    domain: Option<String>,
    /// Path to the pem-encoded certificate (-chain) to use for TLS with client-side certificate
    cert: Option<PathBuf>,
    /// Path to the private key to use for TLS with client-side certificate
    key: Option<PathBuf>,
}

impl TLSClientConfig {
    /// creates a new client config
    #[must_use]
    pub fn new(
        cafile: Option<PathBuf>,
        domain: Option<String>,
        cert: Option<PathBuf>,
        key: Option<PathBuf>,
    ) -> Self {
        TLSClientConfig {
            cafile,
            domain,
            cert,
            key,
        }
    }

    /// the  certificate authority file
    #[must_use]
    pub fn cafile(&self) -> Option<&PathBuf> {
        self.cafile.as_ref()
    }

    /// the domain
    #[must_use]
    pub fn domain(&self) -> Option<&String> {
        self.domain.as_ref()
    }

    /// the certificate file
    #[must_use]
    pub fn cert(&self) -> Option<&PathBuf> {
        self.cert.as_ref()
    }

    /// the key file
    #[must_use]
    pub fn key(&self) -> Option<&PathBuf> {
        self.key.as_ref()
    }

    /// Create a new client connector from the `TLSClientConfig`
    /// if we have a cafile configured, we only load it, and no other ca certificates
    /// if there is no cafile configured, we load the default webpki-roots from Mozilla
    ///
    /// # Errors
    /// if the cafile is invalid

    pub fn to_client_connector(&self) -> Result<TlsConnector, Error> {
        let tls_config = self.to_client_config()?;
        Ok(TlsConnector::from(Arc::new(tls_config)))
    }
    /// Create a new client config from the `TLSClientConfig`
    /// if we have a cafile configured, we only load it, and no other ca certificates
    /// if there is no cafile configured, we load the default webpki-roots from Mozilla
    ///
    /// # Errors
    /// if the cafile is invalid
    pub fn to_client_config(&self) -> Result<ClientConfig, Error> {
        let roots = if let Some(cafile) = self.cafile.as_ref() {
            let mut roots = RootCertStore::empty();
            let certfile = tremor_common::file::open(cafile)?;
            let mut reader = BufReader::new(certfile);

            let cert = rustls_pemfile::read_one(&mut reader)?.and_then(|item| match item {
                Item::X509Certificate(cert) => Some(cert),
                _ => None,
            });
            cert.and_then(|cert| roots.add(cert).ok())
                .ok_or_else(|| Error::InvalidCertificate(cafile.to_string_lossy().to_string()))?;
            roots
        } else {
            SYSTEM_ROOT_CERTS.clone()
        };

        let tls_config = ClientConfig::builder().with_root_certificates(roots);

        // load client certificate stuff
        if let (Some(cert), Some(key)) = (&self.cert, &self.key) {
            let cert = load_certs(cert)?;
            let key = load_keys(key)?;
            Ok(tls_config.with_client_auth_cert(cert, key)?)
        } else {
            Ok(tls_config.with_no_client_auth())
        }
    }
}

/// TLS Errors
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Invalid certificate
    #[error("Invalid certificate file: {0}")]
    InvalidCertificate(String),
    /// Certificate not found
    #[error("Certificate file does not exist: {0}")]
    CertificateNotFound(String),
    /// Invalid private key
    #[error("Invalid private key file: {0}")]
    InvalidPrivateKey(String),
    /// Tremor Common error
    #[error(transparent)]
    TremorCommon(#[from] tremor_common::Error),
    /// TLS error
    #[error(transparent)]
    Tls(#[from] rustls::Error),
    /// IO error
    #[error(transparent)]
    IO(#[from] io::Error),
}

/// Load the passed certificates file
fn load_certs<'x>(path: &PathBuf) -> Result<Vec<CertificateDer<'x>>, Error> {
    let certfile = tremor_common::file::open(&path)?;
    let mut reader = BufReader::new(certfile);

    let certs = rustls_pemfile::certs(&mut reader)
        .map_while(|cert| match cert {
            Ok(cert) => Some(cert),
            Err(_e) => None,
        })
        .collect::<Vec<CertificateDer>>();

    if certs.is_empty() {
        return Err(Error::InvalidCertificate(
            path.to_string_lossy().to_string(),
        ));
    }

    Ok(certs)
}

/// Load the passed private key file
fn load_keys<'x>(path: &PathBuf) -> Result<PrivateKeyDer<'x>, Error> {
    // prefer to load pkcs8 keys
    // this will only error if we have invalid pkcs8 key base64 or we couldnt read the file.
    let keyfile = tremor_common::file::open(&path)?;
    let mut reader = BufReader::new(keyfile);

    let mut keys: Vec<PrivateKeyDer> = pkcs8_private_keys(&mut reader)
        // .map_err(|_e| Error::InvalidPrivateKey(path.to_string_lossy().to_string()))?;
        .map_while(|key| match key {
            Ok(key) => Some(PrivateKeyDer::Pkcs8(key)),
            Err(_e) => None,
        })
        .collect();

    // only attempt to load as RSA keys if file has no pkcs8 keys
    if keys.is_empty() {
        let keyfile = tremor_common::file::open(&path)?;
        let mut reader = BufReader::new(keyfile);
        keys = rsa_private_keys(&mut reader)
            .map_while(|key| match key {
                Ok(key) => Some(PrivateKeyDer::Pkcs1(key)),
                Err(_e) => None,
            })
            .collect();
    }

    keys.pop()
        .ok_or_else(|| Error::InvalidPrivateKey(path.to_string_lossy().to_string()))
}

#[cfg(test)]
mod tests {

    use super::*;
    use std::io::Write;

    #[test]
    fn load_certs_invalid() -> anyhow::Result<()> {
        let mut file = tempfile::NamedTempFile::new()?;
        file.write_all(b"Brueghelflinsch\n")?;
        let path = file.into_temp_path().to_path_buf();
        assert!(load_certs(&path).is_err());
        Ok(())
    }

    #[test]
    fn load_certs_empty() -> anyhow::Result<()> {
        let file = tempfile::NamedTempFile::new()?;
        let path = file.into_temp_path().to_path_buf();
        assert!(load_certs(&path).is_err());
        Ok(())
    }

    #[test]
    fn load_keys_invalid() -> anyhow::Result<()> {
        let mut file = tempfile::NamedTempFile::new()?;
        file.write_all(
            b"-----BEGIN PRIVATE KEY-----\nStrumpfenpfart\n-----END PRIVATE KEY-----\n",
        )?;
        let path = file.into_temp_path().to_path_buf();
        assert!(load_keys(&path).is_err());
        Ok(())
    }

    #[test]
    fn load_keys_empty() -> anyhow::Result<()> {
        let file = tempfile::NamedTempFile::new()?;
        let path = file.into_temp_path().to_path_buf();
        assert!(load_keys(&path).is_err());
        Ok(())
    }

    #[test]
    fn client_config() -> anyhow::Result<()> {
        use std::path::Path;

        use tremor_connectors_test_helpers::setup_for_tls;

        setup_for_tls();
        let tls_config = TLSClientConfig {
            cafile: Some(Path::new("./tests/localhost.cert").to_path_buf()),
            domain: Some("hostenschmirtz".to_string()),
            cert: Some(Path::new("./tests/localhost.cert").to_path_buf()),
            key: Some(Path::new("./tests/localhost.key").to_path_buf()),
        };
        let client_config = tls_config.to_client_config()?;
        assert!(client_config.client_auth_cert_resolver.has_certs());
        Ok(())
    }
}
