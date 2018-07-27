#!/bin/bash

org_id=`curl -X POST -H "Content-Type: application/json" -d '{"name":"data-engineering"}' http://admin:tremor@localhost:3000/api/orgs | jq '.orgId'`
echo "Created organization with id: ${org_id}"

echo "Granting admin to admin in new organization"
curl -X POST http://admin:tremor@localhost:3000/api/user/using/${org_id}

echo "Granting admin to admin in new organization"
curl -X POST -H "Content-Type: application/json" -d '{"loginOrEmail":"admin", "role": "Admin"}' http://admin:tremor@localhost:3000/api/orgs/${org_id}/users

echo "Creating API authorization key for organization admin user"
api_key=`curl -X POST -H "Content-Type: application/json" -d '{"name":"docker-shocker", "role": "Admin"}' http://admin:tremor@localhost:3000/api/auth/keys | jq '.key' | sed -e 's|"||g'`

echo "API KEY: ${api_key}"

echo "Pushing Tremo Demo datasource to grafana"
curl -X POST -H "Authorization: Bearer ${api_key}" -H "Content-Type: application/json" -d '@datasource.json' http://admin:tremor@localhost:3000/api/datasources

echo "Pushing Tremo Demo dashboard to grafana"
curl -X POST -H "Authorization: Bearer ${api_key}" -H "Content-Type: application/json" -d '@dash.json' http://admin:tremor@localhost:3000/api/dashboards/db

echo "Creating InfluxDB database demo"
curl -POST http://localhost:8086/query --data-urlencode "q=CREATE DATABASE demo"
