#!/bin/bash
set -e

cd before
docker-compose up | tee docker-compose.log &
