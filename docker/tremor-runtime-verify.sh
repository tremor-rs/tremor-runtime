#!/bin/sh
#
# docker/example-verify.yaml
#
# A shell script that runs in CI when the example image builds. If it exists
# non-zero, CI will fail. Use this for any testing that linting or Goss doesn't
# cover.
#
# @author Nick Hentschel <nhentschel@wayfair.com>
# @copyright 2018 Wayfair, LLC. -- All rights reserved.

set -o errexit

echo -e "\e[1;31mHello World!\e[0m"
