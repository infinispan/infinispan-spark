package org.infinispan.spark.examples.twitter

import org.apache.spark.SparkConf

object Sample {
   def getSparkConf(appName: String): SparkConf = new SparkConf().setAppName(appName).set("spark.io.compression.codec", "lz4")

}
