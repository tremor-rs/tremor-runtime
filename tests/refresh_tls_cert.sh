#!/bin/bash
# If running from the command line execute from repo root

# Under `cargo test` root will always be `tremor-runtime`
pwd=`pwd`/tests

if [ -f "$pwd/localhost.cert" ]; then  rm -f $pwd/localhost.cert; fi
if [ -f "$pwd/localhost.key" ]; then rm $pwd/localhost.key; fi
openssl req -newkey rsa:2048 -new -nodes -x509 -days 3650 -out $pwd/localhost.cert -keyout $pwd/localhost.key -subj /CN=localhost -config $pwd/openssl.cfg
chmod 664 $pwd/localhost.key
