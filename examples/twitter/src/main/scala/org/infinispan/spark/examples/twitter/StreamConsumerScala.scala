package org.infinispan.spark.examples.twitter

import java.util.Properties
import java.util.concurrent.{Executors, TimeUnit}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.infinispan.client.hotrod.RemoteCacheManager
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder
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
      if (args.length < 4) {
         System.out.println("Usage: StreamConsumerScala <twitter4j.oauth.consumerKey> <twitter4j.oauth.consumerSecret> <twitter4j.oauth.accessToken> twitter4j.oauth.accessTokenSecret")
         System.exit(1)
      }

      Logger.getLogger("org").setLevel(Level.WARN)
      System.setProperty("twitter4j.oauth.consumerKey", args(0))
      System.setProperty("twitter4j.oauth.consumerSecret", args(1))
      System.setProperty("twitter4j.oauth.accessToken", args(2))
      System.setProperty("twitter4j.oauth.accessTokenSecret", args(3))

      val conf = new SparkConf().setAppName("spark-infinispan-stream-consumer-scala")
      val sparkContext = new SparkContext(conf)
      val master = sparkContext.getConf.get("spark.master").replace("spark://", "").replace("mesos://", "").replaceAll(":.*", "")

      val streamingContext = new StreamingContext(sparkContext, Seconds(1))

      val infinispanProperties = new Properties
      infinispanProperties.put("infinispan.client.hotrod.server_list", master)
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

      streamingContext.start()
      streamingContext.awaitTermination()
   }

   object Repeat {
      def every(d: Duration, code: => Unit) =
         Executors.newSingleThreadScheduledExecutor.scheduleWithFixedDelay(new Runnable {
            override def run(): Unit = code
         }, 10, d.toSeconds, TimeUnit.SECONDS)
   }

}

