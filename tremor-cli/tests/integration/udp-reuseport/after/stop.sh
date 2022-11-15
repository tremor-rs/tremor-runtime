#!/bin/bash
if test -f before.pid; then
    kill -QUIT $( cat before.pid )
    # Remove pid file
    rm -f before.pid
fi