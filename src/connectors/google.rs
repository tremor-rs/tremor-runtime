use std::sync::Arc;
use tonic::metadata::MetadataValue;
use tonic::service::Interceptor;
use tonic::{Request, Status};

pub(crate) struct AuthInterceptor {
    pub token: Box<dyn Fn() -> ::std::result::Result<Arc<String>, Status> + Send>,
}

impl Interceptor for AuthInterceptor {
    fn call(&mut self, mut request: Request<()>) -> ::std::result::Result<Request<()>, Status> {
        let header_value = (self.token)()?;
        let metadata_value = match MetadataValue::from_str(header_value.as_str()) {
            Ok(val) => val,
            Err(e) => {
                error!("Failed to get token for BigQuery: {}", e);

                return Err(Status::unavailable(
                    "Failed to retrieve authentication token.",
                ));
            }
        };
        request
            .metadata_mut()
            .insert("authorization", metadata_value);

        Ok(request)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn interceptor_can_add_the_auth_header() {
        let mut interceptor = AuthInterceptor {
            token: Box::new(|| Ok(Arc::new("test".into()))),
        };
        let request = Request::new(());

        let result = interceptor.call(request).unwrap();

        assert_eq!(result.metadata().get("authorization").unwrap(), "test");
    }

    #[test]
    fn interceptor_will_pass_token_error() {
        let mut interceptor = AuthInterceptor {
            token: Box::new(|| Err(Status::unavailable("boo"))),
        };
        let request = Request::new(());

        let result = interceptor.call(request);

        assert_eq!(result.unwrap_err().message(), "boo");
    }

    #[test]
    fn interceptor_fails_on_invalid_token_value() {
        let mut interceptor = AuthInterceptor {
            // control characters (ASCII < 32) are not allowed
            token: Box::new(|| Ok(Arc::new("\r\n".into()))),
        };
        let request = Request::new(());

        let result = interceptor.call(request);

        assert!(result.is_err());
    }
}