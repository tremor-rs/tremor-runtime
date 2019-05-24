#!/bin/sh

if [ -d "$1" ]
then
    path="$1"
else
    path="."
fi

for d in $(remarshal -i $path/Cargo.toml -of json | jq -r '.dependencies | keys []')
do
    dep=$(echo $d | sed -e 's/-/_/g')
    if ! rg "use $dep(::|;| )" $path -trust > /dev/null
    then
        if ! rg "extern crate $dep;" $path -trust > /dev/null
        then
            if ! rg "[^a-z]$dep::" $path -trust > /dev/null
            then
                echo "Not used: $d";
            fi

        fi
    fi
done
