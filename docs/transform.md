```rust
pub fn get_servant(
    (req, data, path): (HttpRequest, Data<State>, Path<(String, String)>),
) -> HTTPResult {
    let url = build_url(&["binding", &path.0, &path.1])?;
    let res0 = data.world.reg.find_binding(&url);
    match res0 {
        Ok(res) => match res {
            Some(res) => reply(req, data, Ok(res.binding), false, 200),
            None => Err(error::ErrorNotFound("Binding not found")),
        },
        Err(e) => Err(error::ErrorInternalServerError(format!(
            "Internal server error: {}",
            e
        ))),
    }
}
```

```rust
pub fn get_servant(
    (req, data, path): (HttpRequest, Data<State>, Path<(String, String)>),
) -> HTTPResult {
    let url = build_url(&["binding", &path.0, &path.1])?;
    let res0 = data
        .world
        .reg
        .find_binding(&url)
        .map_err(|e| error::ErrorInternalServerError(format!("Internal server error: {}", e)))?
        .ok_or_else(|| error::ErrorNotFound("Binding not found"))?;

    reply(req, data, Ok(res0.binding), false, 200)
}
```

```rust
pub fn get_servant(
    (req, data, path): (HttpRequest, Data<State>, Path<(String, String)>),
) -> HTTPResult {
    let url = build_url(&["binding", &path.0, &path.1])?;
    let res0 =
        data.world.reg.find_binding(&url).map_err(|e| {
            error::ErrorInternalServerError(format!("Internal server error: {}", e))
        })?;

    match res {
        Some(res) => reply(req, data, Ok(res.binding), false, 200),
        None => Err(error::ErrorNotFound("Binding not found")),
    }
}
```

```rust
pub fn get_servant(
    (req, data, path): (HttpRequest, Data<State>, Path<(String, String)>),
) -> HTTPResult {
    let url = build_url(&["binding", &path.0, &path.1])?;
    data.world
        .reg
        .find_binding(&url)
        .map_err(|e| error::ErrorInternalServerError(format!("Internal server error: {}", e)))?
        .ok_or_else(|| error::ErrorNotFound("Binding not found"))
        .and_then(|result| reply(req, data, Ok(result.binding), false, 200))
}
```

```rust
pub fn get_servant(
    (req, data, path): (HttpRequest, Data<State>, Path<(String, String)>),
) -> HTTPResult {
    let url = build_url(&["binding", &path.0, &path.1])?;
    data.world
        .reg
        .find_binding(&url)
        .map_err(|e| error::ErrorInternalServerError(format!("Internal server error: {}", e)))
        .and_then(|result| result.ok_or_else(|| error::ErrorNotFound("Binding not found")))
        .and_then(|result| reply(req, data, Ok(result.binding), false, 200))
}
```