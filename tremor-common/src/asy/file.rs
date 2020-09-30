use async_std::{fs::File, path::Path};

use crate::errors::Error;

pub async fn open<S>(path: &S) -> Result<File, Error>
where
    S: AsRef<Path> + ?Sized,
{
    File::open(path).await.map_err(|e| {
        let p: &Path = path.as_ref();
        Error::FileOpen(e, p.to_string_lossy().to_string())
    })
}
