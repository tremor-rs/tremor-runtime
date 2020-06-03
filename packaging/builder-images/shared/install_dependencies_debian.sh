#!/bin/sh

# exit the script when a command fails
set -o errexit

# print each command before executing
set -o xtrace

apt-get update

apt-get install -y \
  cmake        `# for building C deps` \
  libclang-dev `# for onig_sys (via the regex crate)` \
  libssl-dev   `# for openssl (via surf)`

# cleanup
echo apt-get autoremove -y \
  && apt-get clean -y \
  && rm -rf /var/lib/apt/lists/*
