#!/bin/bash

set -e

echo "Obtaining Spark master"
SPARK_MASTER_NAME="twitter_sparkMaster_1"
INFINISPAN_NAME="twitter_infinispan1_1"

STATE=$(docker inspect --format="{{ .State.Running  }}" $SPARK_MASTER_NAME || exit 1;)
if [ "$STATE" == "false" ]
then
  echo "Docker containers not started, exiting..."
  exit 1
fi

SPARK_MASTER="$(docker inspect --format '{{ .NetworkSettings.IPAddress }}' $SPARK_MASTER_NAME)"
INFINISPAN_MASTER="$(docker inspect --format '{{ .NetworkSettings.IPAddress }}' $INFINISPAN_NAME)"

echo "Submitting the job"
docker exec -it $SPARK_MASTER_NAME /usr/local/spark/bin/spark-submit --master spark://$SPARK_MASTER:7077 --class $1 /usr/local/code/infinispan-spark-twitter.jar ${INFINISPAN_MASTER} $2 $3 $4 $5
