#!/bin/sh

if [ -d "$1" ]
then
    path="$1"
else
    path="."
fi

cnt=0
echo "Checking dependencies in $path/Cargo.toml"
for d in $( remarshal -i $path/Cargo.toml -of json | jq -r '.dependencies | keys []' | if  [ -f $path/.depignore ]; then grep -v -f  $path/.depignore; else cat; fi )
do 
    dep=$(echo $d | sed -e 's/-/_/g')
    if ! rg "use $dep(::|;| )" $path -trust > /dev/null
    then
        if ! rg "extern crate $dep;" $path -trust > /dev/null
        then
            if ! rg "[^a-z]$dep::" $path -trust > /dev/null
            then
                cnt=$((cnt + 1))
                echo "Not used: $d";
            fi

        fi
    fi
done

exit $cnt
