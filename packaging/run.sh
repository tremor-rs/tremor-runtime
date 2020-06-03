#!/bin/bash
#
# packaging/run.sh
#
# Package rust project for various targets, across different formats
#
# Meant for use during the final release as part of CI, but can be used for
# local testing/distribution too.
#
# Notes:
# * Package version is auto-inferred from the project version specified in the
#   cargo manifest.
# * Assumes that the target binary is already built (eg: via cross_build.sh or
#   outside of it).
# * For a list of supported formats, see SUPPORTED_FORMATS variable in the script.
#
# Usage: run.sh [-h] [-f FORMATS] TARGET
#   Run `run.sh -h` for more help.
#
# Examples:
#   run.sh x86_64-unknown-linux-gnu                # produce packages for all supported formats
#   run.sh -f deb x86_64-unknown-linux-gnu         # package for debian
#   run.sh -f archive,deb x86_64-unknown-linux-gnu # produce an archive as well as a deb file

# exit the script when a command fails
set -o errexit

# catch exit status for piped commands
set -o pipefail

SUPPORTED_FORMATS="archive,deb"

function print_help {
    cat <<EOF
Usage: ${0##*/} [-h] [-f FORMATS] TARGET
  -h         show this help
  -f FORMATS package format(s). Supported values: ${SUPPORTED_FORMATS}
             To specify multiple formats, pass as a comma-separated string.
EOF
}

###############################################################################

while getopts hf: opt; do
  case $opt in
    h)
      print_help
      exit 0
      ;;
    f)
      FORMATS="$OPTARG"
      ;;
    *)
      print_help
      exit 1
      ;;
  esac
done
shift "$((OPTIND-1))"
TARGET=$@

if [ -z "$TARGET" ]; then
  print_help
  exit 1
fi

# defaults to packaging for all supported formats
if [ -z "$FORMATS" ]; then
  FORMATS="$SUPPORTED_FORMATS"
fi

BIN_NAME="tremor-server"

ROOT_DIR="$(git rev-parse --show-toplevel)"

echo "Packaging ${BIN_NAME} for target ${TARGET} in ${FORMATS} formats, from ${ROOT_DIR}"

# this move allows us to run this script from anywhere in the repo
pushd "$ROOT_DIR" > /dev/null

###############################################################################

TARGET_BUILD_DIR="${ROOT_DIR}/target/${TARGET}/release" # we always package for release builds
TARGET_BIN="$TARGET_BUILD_DIR/${BIN_NAME}"

# assumes that the build's been done first
if [ ! -f "$TARGET_BIN" ]; then
  echo "Could not find the target binary: ${TARGET_BIN}"
  echo "Was the target build successful (eg: via cross_build.sh)?"
  exit 1
fi
echo "Found the target binary: ${TARGET_BIN}"

# get the package version from cargo manifest (assumption is first instance of
# the regex match pattern here is the package version, which is true for most packages)
VERSION=$(grep --max-count 1 '^version\s*=' Cargo.toml | cut --delimiter '=' -f2 | tr --delete ' ' | tr --delete '"' || true)
#
# accurate determination, but depends on remarshal which won't be availale by default
#VERSION=$(remarshal -i Cargo.toml -of json | jq -r '.package.version')

if [ -z "$VERSION" ]; then
  echo "Error: empty package version. Check the project cargo manifest file."
  exit 1
fi
echo "Determined package version to be: ${VERSION}"

# directory to store the final packaged artifacts
PACKAGES_DIR="${ROOT_DIR}/packaging/out"
mkdir -p "$PACKAGES_DIR"

# TODO generate man pages and also add them for packaging

###############################################################################

# include functions from this file
source "${ROOT_DIR}/packaging/functions.sh"

for format in ${FORMATS//,/ }; do
  echo ""
  echo "Working on output format: ${format}"

  case $format in
      archive)
        package_archive
        ;;
      deb)
        package_deb
        ;;
      *)
        echo "Unknown package format '${format}'"
        exit 1
        ;;
  esac
done

echo ""
echo "All was well."

# back to the origin dir, just in case
popd > /dev/null
