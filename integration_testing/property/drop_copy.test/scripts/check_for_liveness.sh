#!/bin/bash

# hygenic bash FTW
set -e
set -u
set -o pipefail

# this script will touch a file in drop_in
# check it has appeared in after_processing
# clean up
#
# it will try 10 times, once a second and then time out

SCRIPT_DIR=$1
IN_DIR="${SCRIPT_DIR}/../drop_in"
AFTER_DIR="${SCRIPT_DIR}/../after_processing"

# give it 30 goes with a second sleep each
for i in {1..10}; do
      CANARY_FILE="canary_touched_empty_file${i}"
      echo "{}" > "${IN_DIR}/${CANARY_FILE}"

      # if this file is processed tremor is alive
      sleep 3s

      if [ ! -z "$(ls -A ${AFTER_DIR})" ]; then
          exit 0; # a good exit
      fi
done

# if we get to here we have failed
echo "tremor runtime is not live"
exit 1
