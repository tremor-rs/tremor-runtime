# Release Process

* Update version in all Cargo.toml files in the repository
  - ./Cargo.toml
  - ./tremor-cli/Cargo.toml
  - ./tremor-common/Cargo.toml
  - ./tremor-script/Cargo.toml
  - ./tremor-influx/Cargo.toml
  - ./tremor-pipeline/Cargo.toml
  - ./tremor-api/Cargo.toml
* Update version in `Dockerfile.learn`.
* CHANGELOG.md
  - Change unreleased section to have the new version for the upcoming release 
* `git tag -a -m"Release v<MAJOR>.<MINOR>.<BUGFIX>" <COMMIT>`
* `git push origin --tag`
* Draft a new release on github
  - This will trigger the docker image build jobs
  - Add a catchy title
  - Add the relevant changelog entries for this release in the description
* Release crates to crates.io:
  - Make sure you are an owner of the crates to publish
  - Execute `cargo publish` in these folders in the following order:
    - ./tremor-common
    - ./tremor-script
* Release https://github.com/tremor-rs/tremor-language-server
  - Bumb version and change dependency to `tremor-script` to the new version.
* Wait for the docker image to build and publish
  - verify docker image with some usage examples
* Go to bed.
