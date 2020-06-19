#!/bin/bash
#
# build_image.sh
#
# Build docker images used for building this rust project
#
# Meant to be run from the same directory as the script
#
# Usage: build_image.sh TARGET
# Example: build_image.sh x86_64-unknown-linux-gnu

# exit the script when a command fails
set -o errexit

# catch exit status for piped commands
set -o pipefail

TARGET=$1
if [ -z "$TARGET" ]; then
  echo "Usage: ${0##*/} TARGET"
  exit 1
fi

# get the full directory path where this script lives
# via https://stackoverflow.com/a/246128
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

echo "Building rust builder image for target ${TARGET}, from ${SCRIPT_DIR}"

# this move allows us to run this script from anywhere in the repo
pushd "$SCRIPT_DIR" > /dev/null

DOCKERFILE="Dockerfile.${TARGET}"
if [ ! -f "$DOCKERFILE" ]; then
  echo "A Dockerfile does not exist for the specified target: ${TARGET}"
  exit 1
fi

IMAGE_NAMESPACE="tremorproject"
IMAGE_NAME="tremor-builder"

RUST_TOOLCHAIN_FILE="../../rust-toolchain"
RUST_VERSION=$(<"$RUST_TOOLCHAIN_FILE")

docker build \
  --network host \
  -t "${IMAGE_NAMESPACE}/${IMAGE_NAME}:${TARGET}" \
  -t "${IMAGE_NAMESPACE}/${IMAGE_NAME}:${TARGET}-${RUST_VERSION}" \
  --build-arg RUST_VERSION=${RUST_VERSION} \
  -f ${DOCKERFILE} \
  .

# back to the origin dir, just in case
popd > /dev/null
