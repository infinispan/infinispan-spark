package org.infinispan.spark.examples.twitter

import java.util.Properties
import java.util.concurrent.{Executors, TimeUnit}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.infinispan.client.hotrod.RemoteCacheManager
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder
import org.infinispan.spark.examples.twitter.Sample.{getSparkConf, runAndExit, usageStream}
import org.infinispan.spark.examples.util.TwitterDStream
import org.infinispan.spark.stream._

import scala.collection.JavaConversions._
import scala.concurrent.duration._
import scala.language.postfixOps

/**
  * @see StreamConsumerJava
  * @author gustavonalle
  */
object StreamConsumerScala {

   def main(args: Array[String]) {
      Logger.getLogger("org").setLevel(Level.WARN)

      if (args.length < 2) {
         usageStream("StreamConsumerScala")
      }

      val infinispanHost = args(0)
      val duration = args(1).toLong * 1000

      val conf = getSparkConf("spark-infinispan-stream-consumer-scala")
      val sparkContext = new SparkContext(conf)

      val streamingContext = new StreamingContext(sparkContext, Seconds(1))

      val infinispanProperties = new Properties
      infinispanProperties.put("infinispan.client.hotrod.server_list", infinispanHost)
      val remoteCacheManager = new RemoteCacheManager(new ConfigurationBuilder().withProperties(infinispanProperties).build())
      val cache = remoteCacheManager.getCache[Long, Tweet]

      val twitterDStream = TwitterDStream.create(streamingContext)

      val keyValueTweetStream = twitterDStream.map(s => (s.getId, s))

      keyValueTweetStream.writeToInfinispan(infinispanProperties)

      Repeat.every(5 seconds, {
         val keySet = cache.keySet()
         val maxKey = keySet.max
         println(s"${keySet.size} tweets inserted in the cache")
         println(s"Last tweet:${Option(cache.get(maxKey)).map(_.getText).getOrElse("<no tweets received so far>")}")
         println()
      })

      runAndExit(streamingContext, duration)
   }

   object Repeat {
      def every(d: Duration, code: => Unit) =
         Executors.newSingleThreadScheduledExecutor.scheduleWithFixedDelay(new Runnable {
            override def run(): Unit = code
         }, 10, d.toSeconds, TimeUnit.SECONDS)
   }

}

