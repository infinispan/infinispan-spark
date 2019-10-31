#!/usr/bin/env bash

set -e -o pipefail -o errtrace -o functrace

function wait_for_ispn() {
  until `docker exec -it ispn-1 sh -c 'curl -s -o /dev/null http://$HOSTNAME:11222/rest/v2/server'`; do
    sleep 3
    echo "Waiting for the server to start..."
  done
}

function create_default_cache() {
  docker exec -it  ispn-1 curl -H "Content-Type: application/json" -d '{"distributed-cache":{"mode":"SYNC"}}' http://$HOSTNAME:11222/rest/v2/caches/default
}

command -v docker-compose >/dev/null 2>&1 || { echo >&2 "docker-compose not installed.  Aborting."; exit 1; }

DEFAULT_INFINISPAN_VERSION=$(cd ../../ && ./sbt --error  'set showSuccess := false' getInfinispanVersion)
DEFAULT_SPARK_VERSION=$(cd ../../ && ./sbt --error  'set showSuccess := false' getSparkVersion)
DEFAULT_SCALA_VERSION=2.12

usage() {

cat << EOF

Usage: ./run-clusters.sh [-i Infinispan version] [-s Spark version] [-c Scala version from Spark]

	-i Infinispan version (default=$DEFAULT_INFINISPAN_VERSION)

	-s Apache Spark version (default=$DEFAULT_SPARK_VERSION)

	-c Scala version that Spark was compiled with

    -h help

EOF

}
while getopts ":i:s:c:h" o; do
    case "${o}" in
        h) usage; exit 0;;
        i)
            i=${OPTARG}
            ;;
        s)
            s=${OPTARG}
            ;;
        c)
            c=${OPTARG}
            ;;
        *)
            usage; exit 0
            ;;
    esac
done
shift $((OPTIND-1))

INFINISPAN_VERSION=${i:-$DEFAULT_INFINISPAN_VERSION}
SPARK_VERSION=${s:-$DEFAULT_SPARK_VERSION}
SCALA_VERSION=${c:-$DEFAULT_SCALA_VERSION}

export INFINISPAN_VERSION SPARK_VERSION SCALA_VERSION

docker-compose up -d

wait_for_ispn "ispn-1"
HOSTNAME=$(docker exec -it  ispn-1 sh -c 'echo $HOSTNAME' | tr -d '\r')
create_default_cache $HOSTNAME
