package org.infinispan.spark.examples.twitter;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import static org.infinispan.spark.examples.twitter.Sample.runAndExit;
import static org.infinispan.spark.examples.twitter.Sample.usage;
import org.infinispan.spark.examples.util.TwitterDStream;
import org.infinispan.spark.stream.InfinispanJavaDStream;
import scala.Tuple2;

import java.util.Collections;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * This demo will start a DStream from Twitter and will save it to Infinispan after applying a transformation.
 * <p>
 * It will then print cache size and last tweet received at regular intervals.
 *
 * @author gustavonalle
 */
public class StreamConsumerJava {

   public static void main(String[] args) {
      if (args.length < 2) {
         usage(StreamConsumerJava.class.getSimpleName());
      }

      String infinispanHost = args[0];
      Long duration = Long.parseLong(args[1]) * 1000;

      // Reduce the log level in the driver
      Logger.getLogger("org").setLevel(Level.WARN);

      SparkConf conf = Sample.getSparkConf("spark-infinispan-stream-consumer-java");

      // Create the streaming context
      JavaStreamingContext javaStreamingContext = new JavaStreamingContext(conf, Seconds.apply(1));

      // Populate infinispan properties
      Properties infinispanProperties = new Properties();
      infinispanProperties.put("infinispan.client.hotrod.server_list", infinispanHost);

      JavaReceiverInputDStream<Tweet> twitterDStream = TwitterDStream.create(javaStreamingContext);

      // Transform from the stream of Tweets to a (K,V) pair
      JavaPairDStream<Long, Tweet> kvPair = twitterDStream.mapToPair(tweet -> new Tuple2<>(tweet.getId(), tweet));

      // Write the stream to infinispan
      InfinispanJavaDStream.writeToInfinispan(kvPair, infinispanProperties);

      // Print cache status every 5 seconds
      CacheStatus cacheStatus = new CacheStatus(infinispanHost);
      cacheStatus.printStatus(5, TimeUnit.SECONDS);

      // Start the processing
      runAndExit(javaStreamingContext.ssc(), duration);
   }

   private static class CacheStatus {

      private final RemoteCache<Long, Tweet> cache;

      CacheStatus(String master) {
         Configuration configuration = new ConfigurationBuilder().addServers(master).create();
         cache = new RemoteCacheManager(configuration).getCache();
      }

      void printStatus(long value, TimeUnit timeUnit) {
         Executors.newSingleThreadScheduledExecutor().scheduleWithFixedDelay(() -> {
            Set<Long> keys = cache.keySet();
            Optional<Long> maxKey = keys.stream().sorted(Collections.reverseOrder()).findFirst();
            System.out.format("%d tweets inserted in the cache\n", keys.size());
            System.out.format("Last tweet:%s\n", maxKey.map(k -> cache.get(k).getText()).orElse("<no tweets received so far>"));
            System.out.println();
         }, 10, value, timeUnit);
      }

   }

}
