#!/bin/sh

set -e

if [ ! -d src ]; then
    mkdir src
fi

cp -r examples src/

echo "[Introduction](introduction.md)\n" > src/SUMMARY.md
echo "  - [1. Passthrough](examples/passthrough/README.md)]" >> src/SUMMARY.md
echo "  - [2. Filter](examples/filter/README.md)]" >> src/SUMMARY.md
echo "  - [3. Transform](examples/transform/README.md)]" >> src/SUMMARY.md
echo "  - [4. Validate](examples/validate/README.md)]" >> src/SUMMARY.md

for f in $(ls text/* | sort)
do
    echo "- [$(basename $f ".md")]($(basename $f))" >> src/SUMMARY.md
    cp $f src
done

cp README.md src/introduction.md

mdbook build
