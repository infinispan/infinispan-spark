package org.infinispan.spark.examples.twitter

import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.infinispan.spark.stream._

/**
 * @author gustavonalle
 */
object StreamProducerScala {
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
      val master = sparkContext.getConf.get("spark.master").replace("spark://", "").replaceAll(":.*", "")

      val streamingContext = new StreamingContext(sparkContext, Seconds(1))

      val infinispanProperties = new Properties
      infinispanProperties.put("infinispan.client.hotrod.server_list", master)

      val twitterDStream = TwitterUtils.createStream(streamingContext, None)

      val keyValueTweetStream = twitterDStream.map {
         s => (s.getId, new Tweet(s.getId, s.getUser.getScreenName, Option(s.getPlace).map(_.getCountry).getOrElse("N/A"), s.getRetweetCount, s.getText))
      }

      keyValueTweetStream.writeToInfinispan(infinispanProperties)

      val infinispanStream = new InfinispanInputDStream[Long, Tweet](streamingContext, StorageLevel.MEMORY_ONLY, infinispanProperties)

      val countryDStream = infinispanStream.transform(rdd => rdd.collect { case (k, v, _)  => (v.getCountry, 1) }.reduceByKey(_ + _))

      val lastMinuteDStream = countryDStream.reduceByKeyAndWindow(_ + _, Seconds(60))

      lastMinuteDStream.foreachRDD { (rdd, time) => println(s"--------- $time -------"); rdd.sortBy(_._2, ascending = false).collect().foreach(println) }

      streamingContext.start()
      streamingContext.awaitTermination()
   }
}
