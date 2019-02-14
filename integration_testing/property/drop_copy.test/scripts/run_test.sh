#!/bin/bash

# hygenic bash FTW
set -e
set -u
set -o pipefail

SCRIPT_DIR=$1

INPUT_FILES="${SCRIPT_DIR}/../input_files/"
IN_FILES="${SCRIPT_DIR}/../drop_in/"

for f in ${INPUT_FILES}*
do
 	cp ${f} ${IN_FILES}
done
