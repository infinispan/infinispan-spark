#!/usr/bin/env bash

set -e -o pipefail -o errtrace -o functrace

function wait_for_ispn() {
  until `docker exec -t $1 /opt/jboss/infinispan-server/bin/ispn-cli.sh -c ":read-attribute(name=server-state)"  | grep -q running`; do
    sleep 3
    echo "Waiting for the server to start..."
  done
}
command -v docker-compose >/dev/null 2>&1 || { echo >&2 "docker-compose not installed.  Aborting."; exit 1; }

DEFAULT_INFINISPAN_VERSION=$(cd ../../ && ./sbt --error  'set showSuccess := false' getInfinispanVersion)
DEFAULT_SPARK_VERSION=$(cd ../../ && ./sbt --error  'set showSuccess := false' getSparkVersion)

usage() {

cat << EOF

Usage: ./run-clusters.sh [-i Infinispan version] [-s Spark version] 

	-i Infinispan version (default=$DEFAULT_INFINISPAN_VERSION)

	-s Apache Spark version (default=$DEFAULT_SPARK_VERSION)

    -h help

EOF

}
while getopts ":i:s:h" o; do
    case "${o}" in
        h) usage; exit 0;;
        i)
            i=${OPTARG}
            ;;
        s)
            s=${OPTARG}
            ;;
        *)
            usage; exit 0
            ;;
    esac
done
shift $((OPTIND-1))

INFINISPAN_VERSION=${i:-$DEFAULT_INFINISPAN_VERSION}
SPARK_VERSION=${s:-$DEFAULT_SPARK_VERSION}

export INFINISPAN_VERSION SPARK_VERSION

docker-compose up -d

wait_for_ispn "ispn-1"
