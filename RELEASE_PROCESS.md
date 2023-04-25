# Release Process


## preperation

* Update version in all Cargo.toml files in the repository
  - ./Cargo.toml
  - ./tremor-cli/Cargo.toml
  - ./tremor-common/Cargo.toml
  - ./tremor-influx/Cargo.toml (if we have changes compared to the previous release)
  - ./tremor-pipeline/Cargo.toml
  - ./tremor-script/Cargo.toml (don't forget the reference to the other tremor packages)
  - ./tremor-value/Cargo.toml (if we have changes compared to the previous release)
* Update version in `./tremor-cli/src/cli.yaml` (minor version update only)
* Update version in `Dockerfile.learn`
* Update CHANGELOG.md
  - Change unreleased section to have the new version for the upcoming release
* Update tests in tremor-cli/tests/ that match against the current version number
  - ./tremor-cli/tests/api/command.yml (the version test)
  - ./tremor-cli/tests/api-cli/command.yml (the version test)
* run `cargo test --all` (this will ensure the Cargo.lock is up to date)
* enter the sub crate directories and run `cargo test` in each one (this is important to make sure they compile stand alone!)
  - `./tremor-common`
  - `./tremor-value`
  - `./tremor-script`
* Create a PR with those changes

## release

* Pull the PR once accepted and merged
* `git tag -a -m"Release v<MAJOR>.<MINOR>.<BUGFIX>" v<MAJOR>.<MINOR>.<BUGFIX> <COMMIT>`
* `git push origin --tag`
* Draft a new release on github
  - This will trigger the docker image build jobs
  - Add a catchy title
  - Add the relevant changelog entries for this release in the description

## follow up

* Release crates to crates.io:
  - Make sure you are an owner of the crates to publish
  - Execute `cargo publish` in these folders in the following order:
    - ./tremor-common
    - ./tremor-value
    - ./tremor-script
* Wait for the docker image to build and publish
  - Verify docker image with some usage examples
  - Tag the published dockerhub image as latest:

  ```sh
  export VERSION=<version>
  docker rmi --force tremorproject/tremor:$VERSION && \
    docker pull tremorproject/tremor:$VERSION && \
    docker tag tremorproject/tremor:$VERSION tremorproject/tremor:latest && \
    docker push tremorproject/tremor:latest
  ```

* Create a corresponding release on tremor-www-docs. See [tremor-www-docs Release Process](https://github.com/tremor-rs/tremor-www-docs/blob/main/RELEASE_PROCESS.md)
* Release https://github.com/tremor-rs/tremor-language-server
  - Bump version and update dependency `tremor-script` to the new version.
  - Checkout the new tremor-www-docs release tag in the `tremor-www-docs` submodule.
  - Create a github tag and draft a release from it:
    - `git tag -a -m"Release v<MAJOR>.<MINOR>.<BUGFIX>" v<MAJOR>.<MINOR>.<BUGFIX> <COMMIT>`
    - `git push origin --tag`
  - Execute `cargo publish` from the language server repository root.
  - Verify new language server installation via `cargo install tremor-language-server`


* If syntax changed: Update the highlighters:
  - https://github.com/tremor-rs/tremor-vim
  - https://github.com/tremor-rs/highlightjs-tremor
  - https://github.com/tremor-rs/tremor-mkdocs-lexer
  - https://github.com/tremor-rs/tremor-vscode
* Go to bed.

## Announcements

Any or all of the following (whichever makes sense)

* Tremor Discord (announcements channel)
* Tremor Twitter
* CNCF Slack Tremor channel
* Tremor Blog