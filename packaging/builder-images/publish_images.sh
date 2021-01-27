#!/bin/bash
#
# publish_images.sh
#
# Publish all the project builder docker images to docker hub
#
# TODO integrate logic here with project Makefile
#
# Usage: ./publish_images.sh

# exit the script when a command fails
set -o errexit

# catch exit status for piped commands
set -o pipefail

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"


RUST_TOOLCHAIN_FILE="${SCRIPT_DIR}/../../rust-toolchain"
RUST_VERSION=$(<"$RUST_TOOLCHAIN_FILE")

IMAGES=(
  "tremor-builder:x86_64-unknown-linux-gnu"
  "tremor-builder:x86_64-unknown-linux-gnu-${RUST_VERSION}"
  "tremor-builder:x86_64-alpine-linux-musl"
  "tremor-builder:x86_64-alpine-linux-musl-${RUST_VERSION}"
  "tremor-builder:x86_64-unknown-linux-musl"
  "tremor-builder:x86_64-unknown-linux-musl-${RUST_VERSION}"
)
for image in "${IMAGES[@]}"; do
  docker push "tremorproject/${image}"
done
