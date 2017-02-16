#!/usr/bin/env bash

BASEDIR=$(dirname "$0")
. $BASEDIR/schema.properties
sr_listeners=http://$sr_hostname:8081
docker run -d --net=host --name=schema-registry -e SCHEMA_REGISTRY_KAFKASTORE_CONNECTION_URL=$zk -e SCHEMA_REGISTRY_HOST_NAME=$sr_hostname -e SCHEMA_REGISTRY_LISTENERS=$sr_listeners -e SCHEMA_REGISTRY_DEBUG=true confluentinc/cp-schema-registry:3.1.1
