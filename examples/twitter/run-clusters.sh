#!/usr/bin/env bash

command -v docker-compose >/dev/null 2>&1 || { echo >&2 "docker-compose not installed.  Aborting."; exit 1; }

INFINISPAN_VERSION=$(cat ../../project/Versions.scala | grep infinispanVersion | awk '{print $4}' | sed 's/\"//g')
SPARK_VERSION=$(cat ../../project/Versions.scala | grep sparkVersion | awk '{print $4}' | sed 's/\"//g')

sed 's/INFINISPAN_VERSION/'$INFINISPAN_VERSION'/g; s/SPARK_VERSION/'$SPARK_VERSION'/g' docker-compose.yml.tt > docker-compose.yml
docker-compose up -d
