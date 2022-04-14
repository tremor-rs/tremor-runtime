#!/bin/bash

echo "If a pid file exists, hard kill"
if test -f before.pid; then
    kill -9 $( cat before.pid )
    # Remove pid file
    rm -f before.pid
fi