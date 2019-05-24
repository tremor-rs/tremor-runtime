#!/bin/bash

tool=`yaml2json ../tremor-tool/src/cli.yaml`

echo "# Tremor tool"
echo
echo `echo $tool | jq -r '.about'`
echo
echo "## Scope"
echo
echo "This document summarises tremor tool commands"
echo
echo "## Audience"
echo
echo "Tremor operators and developers"
echo
echo
echo "## Usage"
echo
echo

api=`yaml2json ../static/openapi.yaml`

echo
echo "### General flags and switches"
echo
echo
echo "|Name|Switch|Kind|Multiple|Description|"
echo "|---|---|---|---|---|"
idxs=`echo $tool | jq ".args | keys[]"`
for flag in $idxs;
do
    elm=`echo $tool | jq -r ".args[$flag] | values[]"`
    name=`echo $tool | jq -r ".args[$flag] | keys[]"`
    short=`echo $elm | jq -r ".short"`
    kind=$(echo $elm | test "`jq -r ".takes_value"`" = "null" && echo switch/flag || echo takes value )
    mult=`echo $elm | jq -r ".multiple" > /dev/null  && echo "yes" || echo "no"`
    desc=`echo $elm | jq -r ".help" || echo "no default"`
    echo "|${name}|${short}|${kind}|${mult}|${desc}|"
done

echo
echo
echo "### Commands"
echo
echo Top level command summary
echo
echo "|Command|Description|"
echo "|---|---|"
idxs=`echo $tool | jq ".subcommands | keys[]"`
for flag in $idxs;
do
    elm=`echo $tool | jq ".subcommands[$flag] | values[]"`
    name=`echo $tool | jq -r ".subcommands[$flag] | keys[]"`
    desc=`echo $elm | jq -r ".about"`
    echo "|${name}|${desc}|"
done

echo
echo
echo "## Command Details"
echo
echo Details for each supported command
for flag in $idxs;
do
    elm=`echo $tool | jq ".subcommands[$flag] | values[]"`
    name=`echo $tool | jq -r ".subcommands[$flag] | keys[]"`
    desc=`echo $elm | jq -r ".about"`
    echo
    echo
    subs=`echo $elm| jq ".subcommands | keys[]"`
    subn=`echo $elm| jq ".subcommands[] | keys[]"`
    echo "### Command: **${name}**"
    echo
    echo ${desc}
    echo
    for sub in $subs;
    do
        subv=`echo $elm | jq ".subcommands[$sub] | values[]"`
        subn=`echo $elm | jq -r ".subcommands[$sub] | keys[]"`
        desc=`echo $subv | jq -r ".about"`
        echo "#### Subcommand: ${subn}"
        echo 
        #echo "${desc}"
        #echo
        if test "${name}" = "api";
        then
          read -r -a idxse <<< `echo $subv | jq ".subcommands | keys[]" || echo ""`
          #echo "LENGTH: ${#idxse[@]} -- $idxse"
          echo
          if test ${#idxse[@]} -ne 0;
          then
              echo "|Name|Description|"
              echo "|---|---|"
              for endpoint in ${idxse[@]};
              do
                  ename=`echo $subv | jq -r ".subcommands[$endpoint] | keys[]"`
                  eelm=`echo $subv | jq -r ".subcommands[$endpoint] | values[]"`
                  edesc=`echo $eelm | jq -r ".about"`
                  echo "|$ename|$edesc|"
              done
              echo
              for endpoint in ${idxse[@]};
              do
                  ename=`echo $subv | jq -r ".subcommands[$endpoint] | keys[]"`
                  eelm=`echo $subv | jq -r ".subcommands[$endpoint] | values[]"`
                  edesc=`echo $eelm | jq -r ".about"`
                  echo
                  echo "##### $subn $ename"
                  echo
                  echo
                  echo "`../target/debug/tremor-tool ${name} ${subn} ${ename} --help | tail -n +2 | sed -E "s/ -+([^ ,]+)/__\1__/g" | sed -E 's|FLAGS:|\`\`\`$$FLAGS:|' | sed -E 's|USAGE:|USAGE:$$\`\`\`|' | sed -E 's|ARGS:|$ARGS:|' | sed -e 's|  <|\&lt;|g' | sed '/^$/d;' | tr '$' '\n' | sed G`"
                  echo
                  echo
                  read -r -a idxs3 <<< `echo $eelm | jq ".args | keys[]" || echo ""`
                  if test ${#idxs3[@]} -ne 0;
                  then
                      echo "__Arguments:__"
                      echo
                      echo
                      echo "|Argument|Required?|Description|"
                      echo "|---|---|---|"
                      for flag in $idxs3;
                      do
                          elm3=`echo $eelm | jq -r ".args[$flag] | values[]"`
                          name3=`echo $eelm | jq -r ".args[$flag] | keys[]"`
                          required3=$(echo $elm3 | test "`jq -r ".required"`" = "true" && echo "yes" || echo "no")
                          desc3=$(echo $elm3 | jq -r ".help")
                          test "${name3}" != "" && echo "|${name3}|${required3}|${desc3}|" || echo "|?|?|"
                      done
                  fi
              done
              echo
          else
              echo
              echo
              echo "`../target/debug/tremor-tool ${name} ${subn} --help | tail -n +2 | sed -E "s/ -+([^ ,]+)/__\1__/g" | sed -E 's|FLAGS:|\`\`\`$$FLAGS:|' | sed -E 's|USAGE:|USAGE:$$\`\`\`|' | sed -E 's|ARGS:|$ARGS:|' | sed 's/  </\&lt;/g' | sed '/^$/d;' | tr '$' '\n' | sed G`"
              echo
              echo
          fi
        fi
        if test "${name}" != "api";
        then
          echo
          echo
          echo "`../target/debug/tremor-tool ${name} ${subn} --help | tail -n +2 | sed -E "s/ -+([^ ,]+)/__\1__/g" | sed -E 's|FLAGS:|\`\`\`$$FLAGS:|' | sed -E 's|USAGE:|USAGE:$$\`\`\`|' | sed -E 's|ARGS:|$ARGS:|' | sed 's/  </\&lt;/g' | sed '/^$/d;' | tr '$' '\n' | sed G`"
          echo
          echo

          idxs2=`echo $subv | jq ".args | keys[]"`
          if test ${#idxs2[@]} -ne 0;
          then
              echo "__Arguments:__"
              echo
              echo
              echo "|Argument|Required?|Description|"
              echo "|---|---|---|"
              for flag in $idxs2;
              do
                  elm2=`echo $subv | jq -r ".args[$flag] | values[]"`
                  name2=`echo $subv | jq -r ".args[$flag] | keys[]"`
                  required=$(echo $elm2 | test "`jq -r ".required"`" = "true" && echo "yes" || echo "no")
                  desc2=$(echo $elm2 | jq -r ".help")
                  test "${name2}" != "" && echo "|${name2}|${required}|${desc2}|" || echo "|?|?|"
              done
              echo
          fi
        fi
    done
done

