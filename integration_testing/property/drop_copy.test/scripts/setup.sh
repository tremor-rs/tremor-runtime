#!/bin/bash

# hygenic bash FTW
set -e
set -u
set -o pipefail

# define functions
function empty_dirs () {
    DIR=$1
    rm -rf ${DIR}
    mkdir -p ${DIR}

}

SCRIPT_DIR=$1
EXP_FILE=$2

# Firstly gotta make the helper module that generates tests
CURRENTDIR=$PWD
cd ./property_testing
cargo build
cd ${CURRENTDIR}

# Secondly clean up (should have happened but might not have)
INPUT_FILES="${SCRIPT_DIR}/../input_files/"
IN_FILES="${SCRIPT_DIR}/../drop_in/"
PROCESSING_FILES="${SCRIPT_DIR}/../during_processing/"
OUT_DIRS="${SCRIPT_DIR}/../after_processing/"

empty_dirs $INPUT_FILES
empty_dirs $IN_FILES
empty_dirs $PROCESSING_FILES
empty_dirs $OUT_DIRS

if [ -f $EXP_FILE ]; then
    rm $EXP_FILE
fi

# Thirdly generate the input files
# the directory path is relative to the runner directory
# currently this is configured with:
# * '-f 10' generate 10 files
# * '-r 10' the target is 10 records per file
# * '-j'    run with jitter - this means each file has a random chance of having between 0 and 2 * 10 records (averaging about 10)
./target/debug/generate_property_tests -d "${SCRIPT_DIR}/../input_files" -f 10 -r 10 -j

# Lastly make the expected output
cat ${INPUT_FILES}* | sort > $EXP_FILE
