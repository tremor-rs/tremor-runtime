#!/bin/sh
# Entrypoint script for our builder images

# enable access to recent gcc version when running on centos images
if [ -f /etc/centos-release ]; then
  source scl_source enable devtoolset-9
fi

set -o xtrace

# diagnostics usful for debugging
id
ldd --version
gcc --version

exec "$@"
