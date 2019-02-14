#!/bin/bash

# hygenic bash FTW
set -e
set -u
set -o pipefail

SCRIPT_DIR=$1
GOT=$2

IN="${SCRIPT_DIR}/../drop_in"
OUT="${SCRIPT_DIR}/../out.log"

# give it a 5 minute timeout
for i in {1..30}; do

      if [ -z "$(ls -A ${IN} | grep -v 'canary')" ]; then
        cat $OUT | grep -v "{}" | sort > $GOT
        # this works because the canaries are empty files
        exit 0; # a good exit
      fi

      sleep 1s

done

echo "something went wrong the drop copy test has timed out"
exit 1
