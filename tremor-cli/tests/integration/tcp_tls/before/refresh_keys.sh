#! /bin/sh
rm localhost.cert localhost.key
time openssl req -newkey rsa:2048 -new -nodes -x509 -days 3650 -out localhost.cert -keyout localhost.key -subj /CN=localhost -config openssl.cfg