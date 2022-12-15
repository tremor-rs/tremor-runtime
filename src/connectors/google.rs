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

use std::{
    fmt::{Display, Formatter},
    task::Poll,
};

use crate::errors::Result;
use async_std::pin::Pin;
use futures::future::Either;
use futures::Future;
use futures::FutureExt;
use google_authz::GoogleAuthz;
use http_body::combinators::UnsyncBoxBody;
use http_body::Body;
use tonic::transport::Channel;
use tower::Service;

#[derive(Debug)]
pub(crate) enum TremorTonicServiceError {
    HyperError(hyper::Error),
    TonicStatus(tonic::Status),
    TonicTransportError(tonic::transport::Error),
    GoogleCredentialsError(google_authz::CredentialsError),
    GoogleAuthError(google_authz::AuthError),
}

// NOTE Channel is an UnsyncBoxBody internally rather than a tonic::transport::Body
// NOTE tonic::transport::Body is an alias for a hyper::Body
// NOTE All leak into the publcic API for tonic - we wrap the distictions way here
// NOTE to avoid leaking the details of the underlying transport and to enable
// NOTE flexible mock testing of any Google gRPC API that uses the same auth in tremor
type TonicInternalBodyType = UnsyncBoxBody<bytes::Bytes, tonic::Status>;

impl Display for TremorTonicServiceError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match *self {
            Self::HyperError(ref err) => write!(f, "Hyper error: {}", err),
            Self::TonicStatus(ref status) => write!(f, "Tonic error: {}", status),
            Self::TonicTransportError(ref err) => write!(f, "Tonic transport error: {}", err),
            Self::GoogleAuthError(ref err) => write!(f, "Google Auth error: {}", err),
            Self::GoogleCredentialsError(ref err) => write!(f, "Google Credentials error: {}", err),
        }
    }
}

impl From<hyper::Error> for TremorTonicServiceError {
    fn from(other: hyper::Error) -> Self {
        Self::HyperError(other)
    }
}

impl From<google_authz::CredentialsError> for TremorTonicServiceError {
    fn from(other: google_authz::CredentialsError) -> Self {
        Self::GoogleCredentialsError(other)
    }
}

impl From<google_authz::AuthError> for TremorTonicServiceError {
    fn from(other: google_authz::AuthError) -> Self {
        Self::GoogleAuthError(other)
    }
}

impl From<tonic::Status> for TremorTonicServiceError {
    fn from(other: tonic::Status) -> Self {
        Self::TonicStatus(other)
    }
}

impl From<google_authz::Error<tonic::transport::Error>> for TremorTonicServiceError {
    fn from(other: google_authz::Error<tonic::transport::Error>) -> Self {
        match other {
            google_authz::Error::Service(err) => Self::TonicTransportError(err),
            google_authz::Error::GoogleAuthz(err) => Self::GoogleAuthError(err),
        }
    }
}

impl From<tonic::transport::Error> for TremorTonicServiceError {
    fn from(other: tonic::transport::Error) -> Self {
        Self::TonicTransportError(other)
    }
}

pub(crate) type MockServiceRpcCall =
    fn(http::Request<TonicInternalBodyType>) -> http::Response<TonicInternalBodyType>;

#[derive(Clone)]
pub(crate) struct TonicMockService {
    delegate: MockServiceRpcCall,
}

impl TonicMockService {
    pub(crate) fn new(delegate: MockServiceRpcCall) -> Self {
        Self { delegate }
    }
}

impl std::error::Error for TremorTonicServiceError {}

impl Service<http::Request<TonicInternalBodyType>> for TonicMockService {
    type Response = http::Response<TonicInternalBodyType>;
    type Error = TremorTonicServiceError;
    type Future = Pin<
        Box<
            dyn Future<
                    Output = std::result::Result<
                        http::Response<TonicInternalBodyType>,
                        TremorTonicServiceError,
                    >,
                > + Send,
        >,
    >;

    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> Poll<std::result::Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<TonicInternalBodyType>) -> Self::Future {
        let response = (self.delegate)(request);
        futures::future::ok(response).boxed()
    }
}

#[derive(Clone)]
pub(crate) enum TremorGoogleAuthz {
    Code(GoogleAuthz<Channel>),
    Test(TonicMockService),
}

// Hoist google-authz errors so we have a common service error whether
// mock testing or in production code for gRPC GCP connectors.
fn map_poll_err<T>(
    result: Poll<std::result::Result<T, google_authz::Error<tonic::transport::Error>>>,
) -> Poll<std::result::Result<T, TremorTonicServiceError>> {
    match result {
        Poll::Ready(Err(google_authz::Error::GoogleAuthz(err))) => Poll::Ready(Err(err.into())),
        Poll::Ready(Err(err @ google_authz::Error::Service(_))) => Poll::Ready(Err(err.into())),
        Poll::Ready(Ok(value)) => Poll::Ready(Ok(value)),
        Poll::Pending => Poll::Pending,
    }
}

type TonicResponseFuture = futures::future::MapErr<
    tonic::transport::channel::ResponseFuture,
    fn(tonic::transport::Error) -> google_authz::Error<tonic::transport::Error>,
>;
type HttpResponseFuture = std::future::Ready<
    std::result::Result<
        http::Response<tonic::transport::Body>,
        google_authz::Error<tonic::transport::Error>,
    >,
>;
type NormalizedResponse =
    std::result::Result<http::Response<TonicInternalBodyType>, TremorTonicServiceError>;

async fn map_ready_err(
    result: Either<TonicResponseFuture, HttpResponseFuture>,
) -> NormalizedResponse {
    match result.await {
        Err(google_authz::Error::GoogleAuthz(err)) => Err(err.into()),
        Err(err @ google_authz::Error::Service(_)) => Err(err.into()),
        Ok(response) => {
            let (_parts, body) = response.into_parts();
            let body = hyper::body::to_bytes(body).await?;
            let body = http_body::Full::new(body);
            let body = http_body::combinators::BoxBody::new(body).map_err(|err| match err {});
            let body = tonic::body::BoxBody::new(body);
            Ok(http::Response::new(body))
        }
    }
}

impl Service<http::Request<TonicInternalBodyType>> for TremorGoogleAuthz {
    type Response = http::Response<TonicInternalBodyType>;
    type Error = TremorTonicServiceError;
    type Future = Pin<
        Box<
            dyn Future<
                    Output = std::result::Result<
                        http::Response<TonicInternalBodyType>,
                        TremorTonicServiceError,
                    >,
                > + Send,
        >,
    >;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<std::result::Result<(), Self::Error>> {
        match self {
            Self::Code(authz) => map_poll_err(authz.poll_ready(cx)),
            Self::Test(authz) => authz.poll_ready(cx),
        }
    }

    fn call(&mut self, req: http::Request<TonicInternalBodyType>) -> Self::Future {
        match self {
            Self::Code(authz) => {
                let result = authz.call(req);
                Box::pin(map_ready_err(result))
            }
            Self::Test(authz) => authz.call(req),
        }
    }
}

impl TremorGoogleAuthz {
    // Create a new live or production channel to a GCP gRPC service.
    pub async fn new(transport: Channel) -> Result<Self> {
        let transport_channel = GoogleAuthz::new(transport).await;
        Ok(Self::Code(transport_channel))
    }

    // Create a new mock channel to a GCP gRPC service
    pub async fn new_mock(logic: MockServiceRpcCall) -> Result<Self> {
        Ok(Self::Test(TonicMockService::new(logic)))
    }
}

#[cfg(test)]
pub(crate) mod tests {

    use super::*;

    fn fake_body(content: Vec<u8>) -> TonicInternalBodyType {
        let body = bytes::Bytes::from(content);
        let body = http_body::Full::new(body);
        let body = http_body::combinators::BoxBody::new(body).map_err(|err| match err {});
        tonic::body::BoxBody::new(body)
    }

    fn empty_body() -> TonicInternalBodyType {
        fake_body(vec![])
    }

    // NOTE We have a public github based CI/CD pipeline for non cloud provider specific protocols
    // NOTE and we have a CNCF equinix env for benchmarks and cloud provider agnostic tests
    // NOTE so we use mock tests where emulators/simulators are not available at this time
    #[async_std::test]
    async fn appease_the_coverage_gods() -> Result<()> {
        let mut mock = TremorGoogleAuthz::new_mock(|_| http::Response::new(empty_body())).await?;
        let actual = mock.call(http::Request::new(empty_body())).await;
        if let Ok(actual) = actual {
            let actual = hyper::body::to_bytes(actual).await?;
            let expected = hyper::body::to_bytes(empty_body()).await?;
            assert_eq!(actual, expected);
        } else {
            return Err("snot".into());
        }
        Ok(())
    }
}
