package org.infinispan.spark.examples.twitter

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext

object Sample {
   def getSparkConf(appName: String): SparkConf = new SparkConf().setAppName(appName)
           .set("spark.io.compression.codec", "lz4")
           .set("spark.sql.warehouse.dir", "/usr/local/code")

   def runAndExit(context: StreamingContext, durationSeconds: Long): Unit = {
      context.start()
      context.awaitTerminationOrTimeout(durationSeconds)
      context.stop(stopSparkContext = false, stopGracefully = true)
      System.exit(0)
   }

   def usage(className: String): Unit = usage(className, twitter = false)

   def usageStream(className: String): Unit = usage(className, twitter = true)

   private def usage(className: String, twitter: Boolean): Unit = {
      println(s"Usage: $className infinispan_host timeoutSeconds")
      if (twitter) {
         println("Twitter OAuth credentials should be set via system properties: ")
         println("-Dtwitter4j.oauth.consumerKey=... -Dtwitter4j.oauth.consumerSecret=... -Dtwitter4j.oauth.accessToken=... -Dtwitter4j.oauth.accessTokenSecret=...")
         System.exit(1)
      }
   }

}
