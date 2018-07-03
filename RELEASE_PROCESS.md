# Release Process

Prior to a release the following steps need to be taken in order:

1. update `Cargo.toml` with the new version of Tremor-Runtime
2. update docker `ARG tag` in `docker/tremor-runtime.dockerfile` with the new version
3. run cargo build to update `Cargo.lock`
4. Update `CHANGELOG.md` with the new version a and changes
5. Run `make release-bench`
6. Push to gitlab
7. Wait for the docker build process to finish (seoncdary pipeline step)
8. Merge when approved
9. Tag the master branch
10. Announce in #data-eng-tremor