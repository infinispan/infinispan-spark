#!/bin/bash

set -e

usage() {
cat << EOF
  Usage: ./run-job.sh -m MainClass [-c Twitter consumer key] [-k Twitter secret key] [-t Twitter access token] [-s Twitter acess token secret] [-d duration(s)] [-v connector Scala version]
   -m Main class of the job
   -c Twitter consumer key
   -k Twitter secret key
   -t Twitter access token
   -s Twitter acess token secret
   -d Maximum duration in seconds to run the job (default: -1, run forever)
   -v Connector Scala version (2.10 or 2.11, default 2.10)
   -h help
EOF
}

while getopts ":k:v:s:d:t:c:m:h" o; do
    case "${o}" in
        h) usage; exit 0;;
        k)
            k=${OPTARG}
            ;;
        v)
            v=${OPTARG}
            ;;
        s)
            s=${OPTARG}
            ;;
        d)
            d=${OPTARG}
            ;;
        t)
            t=${OPTARG}
            ;;
        c)
            c=${OPTARG}
            ;;
        m)
            m=${OPTARG}
            ;;
        *)
            usage; exit 0;;
    esac
done
shift $((OPTIND-1))

if [ ! "$m" ]
then
    echo "ERROR: Main class not specified"
    usage
    exit 1
fi

CLASSNAME=${m}
DURATION=${d:--1}
SCALA_VERSION=${v:-2.10}

echo "Obtaining Spark master"
SPARK_MASTER_NAME="sparkMaster"
INFINISPAN_NAME="ispn-1"

STATE=$(docker inspect --format="{{ .State.Running  }}" $SPARK_MASTER_NAME || exit 1;)
if [ "$STATE" == "false" ]
then
  echo "Docker containers not started, exiting..."
  exit 1
fi

SPARK_MASTER="$(docker inspect --format '{{ .NetworkSettings.IPAddress }}' $SPARK_MASTER_NAME)"
INFINISPAN_MASTER="$(docker inspect --format '{{ .NetworkSettings.IPAddress }}' $INFINISPAN_NAME)"

echo "Submitting the job"
docker exec -it $SPARK_MASTER_NAME /usr/local/spark/bin/spark-submit --driver-java-options "-Dtwitter.oauth.consumerKey=$c -Dtwitter.oauth.consumerSecret=$k -Dtwitter.oauth.accessToken=$t -Dtwitter.oauth.accessTokenSecret=$s" --master spark://$SPARK_MASTER:7077 --class $CLASSNAME /usr/local/code/scala-$SCALA_VERSION/infinispan-spark-twitter.jar ${INFINISPAN_MASTER} ${DURATION}
