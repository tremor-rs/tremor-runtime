#!/bin/sh

set -e

if [ ! -d src ]; then
    mkdir src
fi

echo "[Introduction](introduction.md)\n" > src/SUMMARY.md
echo "  - [1. Passthrough](examples/passthrough/README.md)]" > src/SUMMARY.md
echo "  - [1. Filter](examples/filter/README.md)]" > src/SUMMARY.md
echo "  - [1. Transform](examples/transform/README.md)]" > src/SUMMARY.md
echo "  - [1. Validate](examples/validate/README.md)]" > src/SUMMARY.md

for f in $(ls text/* | sort)
do
    echo "- [$(basename $f ".md")]($(basename $f))" >> src/SUMMARY.md
    cp $f src
done

cp README.md src/introduction.md

mdbook build
