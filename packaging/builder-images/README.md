# Builder Images

Docker images meant to be used for compilation across various targets (eg: as part of CI builds during testing of individual commits/PRs, or to produce the final release artifacts). Works well with [cross](https://github.com/rust-embedded/cross), but can be used independent of it too.

The images here are picked up via the [cross configuration](../Cross.toml) file at project root, during builds.

## Modifications

If you are changing any of the files here, you will need to rebuild and push the image:

```bash
# build image for a target
./build_image.sh x86_64-unknown-linux-gnu

# or the same, from project root
#make builder-image-x86_64-unknown-linux-gnu

# push the image (only project owners have the permissions for this)
docker push anupdhml/example-builder-rust:x86_64-unknown-linux-gnu
```

### Rust Version

The images are provisoned with the rust version specified in the project [rust-toolchain](../rust-toolchain) file, so the above instructions apply for when we bump the versions there too. This can be a point of friction for builds (as the rust version will probably change more frequently than any of the files here). So in the future, we may automate the image updates to keep up with the latest rust releases.
