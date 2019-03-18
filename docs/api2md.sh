#!/bin/bash

echo
echo "## Document status"
echo
echo "Work In Progress"
echo

echo "## Well known API endpoints"
echo
echo "This document summarises the tremor REST API"
echo

api=`yaml2json ../static/openapi.yaml`

echo "|Url|Description|"
echo "|---|---|"
echo $api | jq -r '.servers[] | "|\(.url)\t|\t\(.description)|"'
echo
echo

function to_schema() {
  type=$1;
  yaml2json ../static/openapi.yaml | jq ".components.schemas.$type | { schema: . }"
}

function to_schema_description() {
  type=$1;
  yaml2json ../static/openapi.yaml | jq ".components.schemas.$type.description"
}

## Paths

echo "## Paths "
echo
echo The endpoint paths support by the Tremor REST API
echo
keys=`echo $api | jq -r '.paths | keys[]'`
for path in $keys;
do
  methods=`echo $api | jq -r ".paths.\"${path}\" | keys[]"`
  for method in $methods;
  do
    method_upper=`echo $method | tr a-z A-Z`
    echo "###  __${method_upper}__ ${path}"
    echo
    echo $api | jq -r ".paths.\"${path}\".${method}.summary"
    echo
    echo *Description:*
    echo
    echo $api | jq -r ".paths.\"${path}\".${method}.description"
    echo
    echo "*OperationId:*"
    echo
    echo "> " `echo $api | jq -r ".paths.\"${path}\".${method}.operationId"`
    echo
    # params
    echo "*Returns:*"
    echo
    codes=`echo $api | jq -r ".paths.\"${path}\".${method}.responses | keys[]"`
    echo "> |Status Code|Content Type|Schema Type|"
    echo "> |---|---|---|"
    for status_code in $codes;
    do
      content_types=`echo $api | jq -r ".paths.\"${path}\".${method}.responses.\"${status_code}\".content | keys[]" || echo "empty"`
      for content_type in $content_types;
      do
        if [ "$content_type" == "empty" ];
        then
          echo "> |${status_code}|${content_type}|no content|"
        else
          schema_type=`echo $api | jq -r ".paths.\"${path}\".${method}.responses.\"${status_code}\".content.\"${content_type}\".schema.\"\\\$ref\"" || echo "bug"`
          echo "> |${status_code}|${content_type}|${schema_type}|"
        fi
      done
    done
    echo
  done
done

## Types

echo "## Schemas "
echo
echo JSON Schema for types defined in the Tremor REST API
echo
keys=`echo $api | jq -r '.components.schemas | keys[]'`
for key in $keys;
do
  echo "### Schema for type: __${key}__"
  echo
  echo "$(to_schema_description $key)"
  echo
  echo "\`\`\`json"
  to_schema $key
  echo "\`\`\`"
done

