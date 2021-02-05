#!/usr/bin/env bash
LOCK_DIFF="$(git diff Cargo.lock | wc -l)"

if [ "${LOCK_DIFF}" -ne "0" ]
then
  echo "Build changed the lock file"
  exit 1
fi