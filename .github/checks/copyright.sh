#!/usr/bin/env sh

count=0

for file in $(find . -name '*.rs' | grep -v '/target')
do
    if ! grep 'Copyright 2020, The Tremor Team' "$file" > /dev/null
    then
        echo "##[error] Copyright missing in $file"
        count=$((count + 1))
    fi
done

exit $count
