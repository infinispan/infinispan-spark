#!/usr/bin/env bash

set -e

INFINISPAN_NAME="ispn-1"
SPARK_MASTER_NAME="sparkMaster"
SPARK_MASTER="$(docker inspect --format '{{ .NetworkSettings.IPAddress }}' $SPARK_MASTER_NAME)"
INFINISPAN_IP="$(docker inspect --format '{{ .NetworkSettings.IPAddress }}' $INFINISPAN_NAME)"

docker exec -it $SPARK_MASTER_NAME /usr/local/spark/bin/spark-shell --master spark://$SPARK_MASTER:7077 --jars /usr/local/code/scala-2.12/infinispan-spark-twitter.jar --conf "spark.infinispan=$INFINISPAN_IP"
