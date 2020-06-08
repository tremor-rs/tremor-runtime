#!/bin/bash
#
# cross_build.sh
#
# Build rust project for various targets
#
# Meant for use during the final release as part of CI, but can be used for
# local testing too
#
# Usage: cross_build.sh TARGET
#   For supported TARGET values, see Cross.toml in project root.
#
# Example: cross_build.sh x86_64-unknown-linux-gnu

# exit the script when a command fails
set -o errexit

# catch exit status for piped commands
set -o pipefail

TARGET=$1
if [ -z "$TARGET" ]; then
  echo "Usage: ${0##*/} TARGET"
  exit 1
fi

if [ "$TREMOR_MODE" = "debug" ]; then
  BUILD_MODE=debug
else
  BUILD_MODE=release
fi

BIN_NAME="tremor-server"

ROOT_DIR="$(git rev-parse --show-toplevel)"

echo "Building ${BIN_NAME} for target ${TARGET} in ${BUILD_MODE} mode, from ${ROOT_DIR}"

# this move allows us to run this script from anywhere in the repo
pushd "$ROOT_DIR" > /dev/null

###############################################################################

# install cross if not already there (helps us build easily across various targets)
# see https://github.com/rust-embedded/cross
#
# currently need to install it from a personal fork for builds to work against custom targets
# (eg: x86_64-alpine-linux-musl which we use for generating working musl binaries right now)
# once https://github.com/rust-embedded/cross/pull/431 is merged, we can install it from upstream.
if ! command -v cross > /dev/null 2>&1; then
  echo "Installing cross..."
  cargo install --git https://github.com/anupdhml/cross.git --branch custom_target_fixes
fi

BUILD_ARGS=("--target" "$TARGET")

CUSTOM_RUSTFLAGS=()
RUSTC_TARGET_FEATURES=()

if [ "$BUILD_MODE" == "release" ]; then
  BUILD_ARGS+=("--release")

  # for stripping binaries (saving on size, plus minor performance gains)
  # via https://github.com/rust-lang/cargo/issues/3483#issuecomment-431209957
  #
  # TODO once https://github.com/rust-lang/cargo/issues/3483#issuecomment-631395566
  # lands on a stable rust release, switch to using that
  echo "Ensuring release binaries are stripped..."
  CUSTOM_RUSTFLAGS+=("-C" "link-arg=-s")

  echo "Ensuring simd-json compilation (targetting for CPU instructions it supports)..."
  RUSTC_TARGET_FEATURES+=("+avx" "+avx2" "+sse4.2")
else
  echo "Ensuring simd-json compilation (optimizing for the current CPU)..."
  CUSTOM_RUSTFLAGS+=("-C" "target-cpu=native")
fi

if [[ "$TARGET" == *"alpine-linux-musl"* ]]; then
  # force static binaries for alpine-linux-musl targets (since we are choosing this
  # target specifically to produce working static musl binaries). Static building
  # is the default rustc behavior for musl targets, but alpine disables it by
  # default (via patches to rust).
  echo "Ensuring static builds for alpine-linux-musl targets..."
  RUSTC_TARGET_FEATURES+=("+crt-static")
fi

# if RUSTC_TARGET_FEATURES is not empty, convert it to a comma-separated string
# and append it to CUSTOM_RUSTFLAGS
if [ ${#RUSTC_TARGET_FEATURES[@]} -ne 0 ]; then
  # via https://stackoverflow.com/a/17841619
  function join_by { local IFS="$1"; shift; echo "$*"; }
  CUSTOM_RUSTFLAGS+=( "-C" "target-feature=$(join_by , "${RUSTC_TARGET_FEATURES[@]}")" )
fi

export RUSTFLAGS="${RUSTFLAGS} ${CUSTOM_RUSTFLAGS[@]}"
echo "RUSTFLAGS set to: ${RUSTFLAGS}"

cross build -p tremor-server "${BUILD_ARGS[@]}"

TARGET_BIN="${ROOT_DIR}/target/${TARGET}/${BUILD_MODE}/${BIN_NAME}"

echo "Successfully built the binary: ${TARGET_BIN}"

# linking check
echo "Printing linking information for the binary..."
file "$TARGET_BIN"
ldd "$TARGET_BIN"

# back to the origin dir, just in case
popd > /dev/null
