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

use std::path::{Path, PathBuf};

use crate::errors::{Error, Kind as ErrorKind, Result};
use async_tls::TlsConnector;
use rustls::internal::pemfile::{certs, pkcs8_private_keys, rsa_private_keys};
use rustls::{Certificate, ClientConfig, NoClientAuth, PrivateKey, RootCertStore, ServerConfig};
use rustls_native_certs::load_native_certs;
use std::io::{BufReader, Cursor};

lazy_static! {
    static ref SYSTEM_ROOT_CERTS: RootCertStore = {
        let mut roots = RootCertStore::empty();
        // ALLOW: this is expected to panic if we cannot load system certificates
        for cert in load_native_certs().expect("Unable to load system TLS certificates.") {
            roots
                .add(&Certificate(cert.0))
                // ALLOW: this is expected to panic if we cannot load system certificates
                .expect("Unable to add root TLS certificate to RootCertStore");
        }
        roots
    };
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct TLSServerConfig {
    pub(crate) cert: PathBuf,
    pub(crate) key: PathBuf,
}

#[derive(Deserialize, Debug, Default, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct TLSClientConfig {
    /// Path to the pem-encoded certificate file of the CA to use for verifying the servers certificate
    pub(crate) cafile: Option<PathBuf>,
    /// The DNS domain used to verify the server's certificate. If not provided the domain from the connection URL will be used.
    pub(crate) domain: Option<String>,
    /// Path to the pem-encoded certificate (-chain) to use for TLS with client-side certificate
    pub(crate) cert: Option<PathBuf>,
    /// Path to the private key to use for TLS with client-side certificate
    pub(crate) key: Option<PathBuf>,
}

/// Load the passed certificates file
pub(crate) fn load_certs(path: &Path) -> Result<Vec<Certificate>> {
    let certfile = tremor_common::file::open(path)?;
    let mut reader = BufReader::new(certfile);
    certs(&mut reader)
        .map_err(|_| {
            Error::from(ErrorKind::TLSError(format!(
                "Invalid certificate in {}",
                path.display()
            )))
        })
        .and_then(|certs| {
            if certs.is_empty() {
                Err(Error::from(ErrorKind::TLSError(format!(
                    "No valid TLS certificates found in {}",
                    path.display()
                ))))
            } else {
                Ok(certs)
            }
        })
}

/// Load the passed private key file
pub(crate) fn load_keys(path: &Path) -> Result<PrivateKey> {
    // prefer to load pkcs8 keys
    // this will only error if we have invalid pkcs8 key base64 or we couldnt read the file.
    let mut keys: Vec<PrivateKey> = {
        let keyfile = tremor_common::file::open(path)?;
        let mut reader = BufReader::new(keyfile);
        pkcs8_private_keys(&mut reader).map_err(|_e| {
            Error::from(ErrorKind::TLSError(format!(
                "Invalid PKCS8 Private key in {}",
                path.display()
            )))
        })
    }?;

    // only attempt to load as RSA keys if file has no pkcs8 keys
    if keys.is_empty() {
        let keyfile = tremor_common::file::open(path)?;
        let mut reader = BufReader::new(keyfile);
        keys = rsa_private_keys(&mut reader).map_err(|_e| {
            Error::from(ErrorKind::TLSError(format!(
                "Invalid RSA Private key in {}",
                path.display()
            )))
        })?;
    }

    if keys.is_empty() {
        Err(Error::from(ErrorKind::TLSError(format!(
            "No valid private keys (RSA or PKCS8) found in {}",
            path.display()
        ))))
    } else {
        // ALLOW: we know keys is not empty
        Ok(keys.remove(0))
    }
}

pub(crate) fn load_server_config(config: &TLSServerConfig) -> Result<ServerConfig> {
    let certs = load_certs(&config.cert)?;

    let keys = load_keys(&config.key)?;

    let mut server_config = ServerConfig::new(NoClientAuth::new());
    server_config
        // set this server to use one cert together with the loaded private key
        .set_single_cert(certs, keys)?;

    Ok(server_config)
}

/// if we have a cafile configured, we only load it, and no other ca certificates
/// if there is no cafile configured, we load the default webpki-roots from Mozilla
pub(crate) async fn tls_client_connector(config: &TLSClientConfig) -> Result<TlsConnector> {
    let tls_config = tls_client_config(config).await?;
    Ok(TlsConnector::from(tls_config))
}

pub(crate) async fn tls_client_config(tremor_config: &TLSClientConfig) -> Result<ClientConfig> {
    let mut tls_config = ClientConfig::new();
    // load server cert verification stuff
    if let Some(cafile) = tremor_config.cafile.as_ref() {
        let file = async_std::fs::read(cafile).await?;
        let mut pem = Cursor::new(file);
        tls_config.root_store.add_pem_file(&mut pem).map_err(|_e| {
            Error::from(ErrorKind::TLSError(format!(
                "Invalid certificate in {}",
                cafile.display()
            )))
        })?;
    } else {
        tls_config.root_store = SYSTEM_ROOT_CERTS.clone();
    }
    // load client certificate stuff
    if let (Some(cert), Some(key)) = (tremor_config.cert.as_ref(), tremor_config.key.as_ref()) {
        let cert = load_certs(cert)?;
        let key = load_keys(key)?;
        tls_config.set_single_client_cert(cert, key)?;
    }
    Ok(tls_config)
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use crate::connectors::tests::setup_for_tls;

    use super::*;

    #[test]
    fn load_certs_invalid() -> Result<()> {
        let mut file = tempfile::NamedTempFile::new()?;
        file.write_all(b"Brueghelflinsch\n")?;
        let path = file.into_temp_path();
        assert!(load_certs(&path).is_err());
        Ok(())
    }

    #[test]
    fn load_certs_empty() -> Result<()> {
        let file = tempfile::NamedTempFile::new()?;
        let path = file.into_temp_path();
        assert!(load_certs(&path).is_err());
        Ok(())
    }

    #[test]
    fn load_keys_invalid() -> Result<()> {
        let mut file = tempfile::NamedTempFile::new()?;
        file.write_all(
            b"-----BEGIN PRIVATE KEY-----\nStrumpfenpfart\n-----END PRIVATE KEY-----\n",
        )?;
        let path = file.into_temp_path();
        assert!(load_keys(&path).is_err());
        Ok(())
    }

    #[test]
    fn load_keys_empty() -> Result<()> {
        let file = tempfile::NamedTempFile::new()?;
        let path = file.into_temp_path();
        assert!(load_keys(&path).is_err());
        Ok(())
    }

    #[async_std::test]
    async fn client_config() -> Result<()> {
        setup_for_tls();

        let tls_config = TLSClientConfig {
            cafile: Some(Path::new("./tests/localhost.cert").to_path_buf()),
            domain: Some("hostenschmirtz".to_string()),
            cert: Some(Path::new("./tests/localhost.cert").to_path_buf()),
            key: Some(Path::new("./tests/localhost.key").to_path_buf()),
        };
        let client_config = tls_client_config(&tls_config).await?;
        assert_eq!(1, client_config.root_store.roots.len());
        assert_eq!(true, client_config.client_auth_cert_resolver.has_certs());
        Ok(())
    }
}
