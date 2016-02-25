#!/usr/bin/env bash

function wait_for_ispn() {
  until `docker exec -t $1 /opt/jboss/infinispan-server/bin/ispn-cli.sh -c ":read-attribute(name=server-state)"  | grep -q running`; do
    sleep 3
    echo "Waiting for the server to start..."
  done
}
command -v docker-compose >/dev/null 2>&1 || { echo >&2 "docker-compose not installed.  Aborting."; exit 1; }

INFINISPAN_VERSION=$(cd ../../ && sbt --error  'set showSuccess := false' getInfinispanVersion)
SPARK_VERSION=$(cd ../../ && sbt --error  'set showSuccess := false' getSparkVersion)

export INFINISPAN_VERSION SPARK_VERSION

docker-compose up -d

wait_for_ispn "ispn-1"
