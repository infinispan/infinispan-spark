package org.infinispan.spark.examples.twitter

import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.infinispan.spark.examples.twitter.Sample.{getSparkConf, runAndExit, usageStream}
import org.infinispan.spark.examples.util.TwitterDStream
import org.infinispan.spark.stream._

/**
  * @author gustavonalle
  */
object StreamProducerScala {
   def main(args: Array[String]) {
      Logger.getLogger("org").setLevel(Level.WARN)

      if (args.length < 2) {
         usageStream("StreamProducerScala")
      }

      val infinispanHost = args(0)
      val duration = args(1).toLong * 1000

      val conf = getSparkConf("spark-infinispan-stream-producer-scala")
      val sparkContext = new SparkContext(conf)

      val streamingContext = new StreamingContext(sparkContext, Seconds(1))

      val infinispanProperties = new Properties
      infinispanProperties.put("infinispan.client.hotrod.server_list", infinispanHost)

      val twitterDStream = TwitterDStream.create(streamingContext)

      val keyValueTweetStream = twitterDStream.map(s => (s.getId, s))

      keyValueTweetStream.writeToInfinispan(infinispanProperties)

      val infinispanStream = new InfinispanInputDStream[Long, Tweet](streamingContext, StorageLevel.MEMORY_ONLY, infinispanProperties)

      val countryDStream = infinispanStream.transform(rdd => rdd.collect { case (k, v, _) => (v.getCountry, 1) }.reduceByKey(_ + _))

      val lastMinuteDStream = countryDStream.reduceByKeyAndWindow(_ + _, Seconds(60))

      lastMinuteDStream.foreachRDD { (rdd, time) => println(s"--------- $time -------"); rdd.sortBy(_._2, ascending = false).collect().foreach(println) }

      runAndExit(streamingContext, duration)

   }
}
