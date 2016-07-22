package org.infinispan.spark.examples.twitter

import java.util.Properties
import java.util.concurrent.{Executors, TimeUnit}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.infinispan.client.hotrod.RemoteCacheManager
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder
import org.infinispan.spark.examples.twitter.Sample.{getSparkConf, runAndExit}
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
      if (args.length < 5) {
         System.out.println("Usage: StreamConsumerScala <infinispan_host> <twitter4j.oauth.consumerKey> <twitter4j.oauth.consumerSecret> <twitter4j.oauth.accessToken> <twitter4j.oauth.accessTokenSecret> <timeout (s)>")
         System.exit(1)
      }

      Logger.getLogger("org").setLevel(Level.WARN)
      val infinispanHost = args(0)
      System.setProperty("twitter4j.oauth.consumerKey", args(1))
      System.setProperty("twitter4j.oauth.consumerSecret", args(2))
      System.setProperty("twitter4j.oauth.accessToken", args(3))
      System.setProperty("twitter4j.oauth.accessTokenSecret", args(4))
      val durationOptional: Long = if (args.length > 5) args(5).toLong * 1000 else -1

      val conf = getSparkConf("spark-infinispan-stream-consumer-scala")
      val sparkContext = new SparkContext(conf)

      val streamingContext = new StreamingContext(sparkContext, Seconds(1))

      val infinispanProperties = new Properties
      infinispanProperties.put("infinispan.client.hotrod.server_list", infinispanHost)
      val remoteCacheManager = new RemoteCacheManager(new ConfigurationBuilder().withProperties(infinispanProperties).build())
      val cache = remoteCacheManager.getCache[Long, Tweet]

      val twitterDStream = TwitterUtils.createStream(streamingContext, None)

      val keyValueTweetStream = twitterDStream.map {
         s => (s.getId, new Tweet(s.getId, s.getUser.getScreenName, Option(s.getPlace).map(_.getCountry).getOrElse("N/A"), s.getRetweetCount, s.getText))
      }

      keyValueTweetStream.writeToInfinispan(infinispanProperties)

      Repeat.every(5 seconds, {
         val keySet = cache.keySet()
         val maxKey = keySet.max
         println(s"${keySet.size} tweets inserted in the cache")
         println(s"Last tweet:${Option(cache.get(maxKey)).map(_.getText).getOrElse("<no tweets received so far>")}")
         println()
      })

      runAndExit(streamingContext, durationOptional)
   }

   object Repeat {
      def every(d: Duration, code: => Unit) =
         Executors.newSingleThreadScheduledExecutor.scheduleWithFixedDelay(new Runnable {
            override def run(): Unit = code
         }, 10, d.toSeconds, TimeUnit.SECONDS)
   }

}

