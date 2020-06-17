#!/usr/bin/env bash
count=0

help() {
    cat <<EOF
Usage: ${0##*/} [-hd] [TEST]...
code sanity checker
  -h         show this help
  -a         run all chekcs
  -u         check for unwrap
  -i         check for unimplemented
  -r         check for unreachable
  -p         check for panic
  -l         check for let _
  -e         check for expect
  -d         check for dbg!
  -x         check for std::process::exit
  -b         check for bracket access
  -c         check for pedantic and other checks
EOF
}



files=$(find . -name '*.rs' | grep -v -f .checkignore)

while getopts hauiprebldxc opt; do
    case $opt in
        h)
            help
            exit 0
            ;;
        a)
            exec "$0" -uirpeldxc
            ;;
        u)
            for file in $files
            do
                if sed -e '/mod test.*/,$d' -e '/ALLOW: /{N;d;}' "$file" | grep 'unwrap()' > /dev/null
                then
                    echo "##[error] unwrap found in $file don't unwrap it panics."
                    count=$((count + 1))
                fi
            done
            ;;
        i)
            for file in $files
            do
                if sed -e '/mod test.*/,$d'  "$file" | grep 'unimplemented!' > /dev/null
                then
                    echo "##[error] unimplemented! found in $file please implement."
                    grep -nH 'unimplemented!' "$file"
                    count=$((count + 1))
                fi
            done
            ;;
        l)
            for file in $files
            do
                if sed -e '/mod test.*/,$d' -e '/ALLOW: /{N;d;}' "$file" | grep 'let _' > /dev/null
                then
                    echo "##[error] 'let _' found in $file please use error handling."
                    grep -nH 'let _' "$file"
                    count=$((count + 1))
                fi
            done
            ;;
        r)
            for file in $files
            do
                if sed -e '/mod test.*/,$d' -e '/ALLOW: /{N;d;}' "$file" | grep 'unreachable!' > /dev/null
                then
                    echo "##[error] unreachable! found in $file please don't."
                    grep -nH 'unreachable!' "$file"
                    count=$((count + 1))
                fi
            done
            ;;  
        d)
            for file in $files
            do
                if sed -e '/mod test.*/,$d' -e '/ALLOW: /{N;d;}' "$file" | grep 'dbg!' > /dev/null
                then
                    echo "##[error] dbg! found in $file please use error!, info! etc instead."
                    grep -nH 'dbg!' "$file"
                    count=$((count + 1))
                fi
            done
            ;;  

        x)
            for file in $files
            do
                if sed -e '/mod test.*/,$d' -e '/ALLOW: /{N;d;}' "$file" | grep 'exit(' > /dev/null
                then
                    echo "##[error] exit(_) found in $file please don't ever do that."
                    grep -nH 'exit(' "$file"
                    count=$((count + 1))
                fi
            done
            ;;                  
        p)
            for file in $files
            do
                if sed -e '/mod test.*/,$d' -e '/ALLOW: /{N;d;}' "$file" | grep 'panic!(' > /dev/null
                then
                    echo "##[error] panic found in $file no, just no!"
                    grep -nH 'panic!(' "$file"
                    count=$((count + 1))
                fi
            done
            ;;
        e)
            for file in $files
            do
                if sed -e '/mod test.*/,$d' -e '/ALLOW: /{N;d;}' "$file" | grep 'expect(' > /dev/null
                then
                    echo "##[error] expect found in $file try hygenic errors, this panics!"
                    count=$((count + 1))
                fi
            done
            ;;
        b)
            for file in $files
            do
                if sed -e '/mod test.*/,$d' -e '/ALLOW: /{N;d;}' "$file" | grep '[a-z]\[' > /dev/null
                then
                    echo "##[error] array access ([...]) found in $file that could go wrong, array access can panic."
                    count=$((count + 1))
                fi
            done
            ;;
        c)
            files=$(find . -name 'lib.rs' -or -name 'main.rs' | grep -v -f .checkignore)
            for file in $files
            do
                if  ! grep 'clippy::pedantic' "$file" > /dev/null
                then
                    echo "##[error] $file does not enforce clippy::pedantic."
                    count=$((count + 1))
                fi
                if  ! grep 'clippy::result_unwrap_used' "$file" > /dev/null
                then
                    echo "##[error] $file does not enforce clippy::result_unwrap_used."
                    count=$((count + 1))
                fi
                if  ! grep 'clippy::option_unwrap_used' "$file" > /dev/null
                then
                    echo "##[error] $file does not enforce clippy::option_unwrap_used."
                    count=$((count + 1))
                fi
                if  ! grep 'clippy::unnecessary_unwrap' "$file" > /dev/null
                then
                    echo "##[error] $file does not enforce clippy::unnecessary_unwrap."
                    count=$((count + 1))
                fi
                if  ! grep 'clippy::all' "$file" > /dev/null
                then
                    echo "##[error] $file does not enforce clippy::all."
                    count=$((count + 1))
                fi
                # if  grep 'clippy::missing_errors_doc' "$file" > /dev/null
                # then
                #     echo "##[error] $file does not enforce clippy::missing_errors_doc is mentioend but shouldn't be allowed."
                #     count=$((count + 1))
                # fi
                if  ! grep 'missing_docs' "$file" > /dev/null
                then
                    echo "##[error] $file does not enforce missing_docs."
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