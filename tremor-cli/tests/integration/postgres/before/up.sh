#!/bin/bash
set -e

cd before
docker-compose up | tee docker-compose.log &

sleep 60
echo ERROR Suspected hang in docker-compose
docker-compose down

exit 0
