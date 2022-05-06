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
