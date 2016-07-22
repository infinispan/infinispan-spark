package org.infinispan.spark.examples.twitter

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext

object Sample {
   def getSparkConf(appName: String): SparkConf = new SparkConf().setAppName(appName).set("spark.io.compression.codec", "lz4")

   def runAndExit(context: StreamingContext, durationSeconds: Long): Unit = {
      context.start()
      context.awaitTerminationOrTimeout(durationSeconds)
      context.stop(stopSparkContext = false, stopGracefully = true)
      System.exit(0)
   }

}
