# Release Process

Prior to a release the following steps need to be taken in order:

1. update `Cargo.toml` with the new version
2. update docker `ARG tag` in `docker/tremor-runtime.dockerfile` with the new version
2. run cargo build to update `Cargo.lock`
3. Update `CHANGELOG.md` with the new version a and changes
4. Run `make release-bench`
5. Push to gitlab
6. Wait for the docker build process to finish (seoncdary pipeline step)
7. Merge when approved
8. Annouce in #data-eng-tremor