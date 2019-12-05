#!/usr/bin/env sh

count=0

for file in $(find . -name '*.rs' | grep -v '/target')
do
    if ! grep 'Copyright 2018-2020, Wayfair GmbH' "$file" > /dev/null
    then
        echo "##[error] Copyright missing in $file"
        count=$((count + 1))
    fi
done

exit $count
