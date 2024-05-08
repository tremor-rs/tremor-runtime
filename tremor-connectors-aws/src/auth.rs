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
use aws_config::ConfigLoader;
use aws_credential_types::{provider::future, Credentials};
use aws_sdk_s3::config::ProvideCredentials;
use serde::Deserialize;

use crate::EndpointConfig;

/// AWS authentication configuration
#[derive(Deserialize, Debug, Clone, Default, PartialEq)]
pub(crate) enum AWSAuth {
    /// Defalt configuratuin, uses defaults as described in the [aws docs](https://docs.aws.amazon.com/sdk-for-rust/latest/dg/credentials.html#providing-credentials)
    #[default]
    Default,
    /// Custom access key configuration
    AccessKey {
        /// AWS access key id
        access_key_id: String,
        /// AWS secret access key
        secret_access_key: String,
        /// Optional session token
        session_token: Option<String>,
        // /// Optional expiration time - We do not expose this as there is no meaningful functionality to it
        // expires_after: Option<SystemTime>,
    },
}

#[derive(Debug)]
struct CredentialProvider {
    access_key_id: String,
    secret_access_key: String,
    session_token: Option<String>,
}

impl ProvideCredentials for CredentialProvider {
    fn provide_credentials<'a>(&'a self) -> future::ProvideCredentials<'a>
    where
        Self: 'a,
    {
        future::ProvideCredentials::new(async {
            let creds = Credentials::new(
                &self.access_key_id,
                &self.secret_access_key,
                self.session_token.clone(),
                None,
                "tremor",
            );
            Ok(creds)
        })
    }
}

pub(crate) fn resolve(config: &EndpointConfig, sdk_config: ConfigLoader) -> ConfigLoader {
    match &config.auth {
        AWSAuth::Default => sdk_config,
        AWSAuth::AccessKey {
            access_key_id,
            secret_access_key,
            session_token,
        } => sdk_config.credentials_provider(CredentialProvider {
            access_key_id: access_key_id.clone(),
            secret_access_key: secret_access_key.clone(),
            session_token: session_token.clone(),
        }),
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use anyhow::Result;
    use tremor_value::{literal, structurize};

    #[test]
    fn test_default() -> Result<()> {
        let auth: AWSAuth = structurize(literal!("Default"))?;

        assert_eq!(auth, AWSAuth::Default);
        Ok(())
    }

    #[test]
    fn test_acces_key() -> Result<()> {
        let auth: AWSAuth = structurize(
            literal!({"AccessKey":{"access_key_id":"key id","secret_access_key":"secret"}}),
        )?;

        assert_eq!(
            auth,
            AWSAuth::AccessKey {
                access_key_id: "key id".to_string(),
                secret_access_key: "secret".to_string(),
                session_token: None,
            }
        );
        Ok(())
    }

    #[test]
    fn test_acces_key_token() -> Result<()> {
        let auth: AWSAuth = structurize(
            literal!({"AccessKey":{"access_key_id":"key id","secret_access_key":"secret", "session_token":"token"}}),
        )?;

        assert_eq!(
            auth,
            AWSAuth::AccessKey {
                access_key_id: "key id".to_string(),
                secret_access_key: "secret".to_string(),
                session_token: Some("token".to_string()),
            }
        );
        Ok(())
    }

    #[tokio::test]
    async fn resolve_key() -> Result<()> {
        let config = EndpointConfig {
            aws_region: None,
            url: None,
            auth: AWSAuth::AccessKey {
                access_key_id: "key id".to_string(),
                secret_access_key: "secret".to_string(),
                session_token: None,
            },
            path_style_access: true,
        };

        let sdk_config = aws_config::defaults(aws_sdk_s3::config::BehaviorVersion::latest());

        let resolved = resolve(&config, sdk_config).load().await;

        assert!(resolved.credentials_provider().is_some());
        let credentials = resolved
            .credentials_provider()
            .expect("credentials provider")
            .provide_credentials()
            .await?;

        assert_eq!(credentials.access_key_id(), "key id");
        assert_eq!(credentials.secret_access_key(), "secret");
        assert_eq!(credentials.session_token(), None);
        Ok(())
    }

    #[tokio::test]
    async fn resolve_key_with_token() -> Result<()> {
        let config = EndpointConfig {
            aws_region: None,
            url: None,
            auth: AWSAuth::AccessKey {
                access_key_id: "key id".to_string(),
                secret_access_key: "secret".to_string(),
                session_token: Some("token".to_string()),
            },
            path_style_access: true,
        };

        let sdk_config = aws_config::defaults(aws_sdk_s3::config::BehaviorVersion::latest());

        let resolved = resolve(&config, sdk_config).load().await;

        assert!(resolved.credentials_provider().is_some());
        let credentials = resolved
            .credentials_provider()
            .expect("credentials provider")
            .provide_credentials()
            .await?;

        assert_eq!(credentials.access_key_id(), "key id");
        assert_eq!(credentials.secret_access_key(), "secret");
        assert_eq!(credentials.session_token(), Some("token"));

        Ok(())
    }
}
