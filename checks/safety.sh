#!/usr/bin/env bash
count=0

help() {
    cat <<EOF
Usage: ${0##*/} [-hd] [-t TARGET] [-c CMD] [TEST]...
code sanity checker
  -h         show this help
  -a         run all chekcs
  -u         check for unwrap
  -i         check for unimplemented
  -r         check for unreachable
  -p         check for panic
  -e         check for expect
  -b         bracket access
EOF
}



files=$(find . -name '*.rs' | grep -v -f .checkignore)

while getopts hauipreb opt; do
    case $opt in
        h)
            help
            exit 0
            ;;
        a)
            exec "$0" -uipe
            ;;
        u)
            for file in $files
            do
                if sed -e '/mod test.*/,$d' -e '/ALLOW: /{N;d;}' "$file" | grep 'unwrap()' > /dev/null
                then
                    echo "##[error] unwrap found in $file"
                    count=$((count + 1))
                fi
            done
            ;;
        i)
            for file in $files
            do
                if sed -e '/mod test.*/,$d' "$file" | grep 'unimplemented!' > /dev/null
                then
                    echo "##[error] unimplemented! found in $file"
                    count=$((count + 1))
                fi
            done
            ;;
        r)
            for file in $files
            do
                if sed -e '/mod test.*/,$d' "$file" | grep 'unreachable!' > /dev/null
                then
                    echo "##[error] unreachable! found in $file"
                    count=$((count + 1))
                fi
            done
            ;;        
        p)
            for file in $files
            do
                if sed -e '/mod test.*/,$d' "$file" | grep 'panic!(' > /dev/null
                then
                    echo "##[error] panic found in $file"
                    count=$((count + 1))
                fi
            done
            ;;
        e)
            for file in $files
            do
                if sed -e '/mod test.*/,$d' -e '/ALLOW: /{N;d;}' "$file" | grep 'expect(' > /dev/null
                then
                    echo "##[error] expect found in $file"
                    count=$((count + 1))
                fi
            done
            ;;
        b)
            for file in $files
            do
                if sed -e '/mod test.*/,$d' -e '/ALLOW: /{N;d;}' "$file" | grep '[a-z]\[' > /dev/null
                then
                    echo "##[error] array access ([...]) found in $file"
                    count=$((count + 1))
                fi
            done
            ;;
        *)
            help
            exit 1
            ;;
    esac
done

echo "Found $count problems"
exit $count