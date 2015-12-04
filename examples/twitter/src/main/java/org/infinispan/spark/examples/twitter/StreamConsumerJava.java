package org.infinispan.spark.examples.twitter;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.spark.stream.InfinispanJavaDStream;
import scala.Tuple2;
import twitter4j.Place;
import twitter4j.Status;

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
      if (args.length < 5) {
         System.out.println("Usage: StreamConsumerJava <infinispan_host> <twitter4j.oauth.consumerKey> <twitter4j.oauth.consumerSecret> <twitter4j.oauth.accessToken> <twitter4j.oauth.accessTokenSecret>");
         System.exit(1);
      }

      String infinispanHost = args[0];
      System.setProperty("twitter4j.oauth.consumerKey", args[1]);
      System.setProperty("twitter4j.oauth.consumerSecret", args[2]);
      System.setProperty("twitter4j.oauth.accessToken", args[3]);
      System.setProperty("twitter4j.oauth.accessTokenSecret", args[4]);

      // Reduce the log level in the driver
      Logger.getLogger("org").setLevel(Level.WARN);

      SparkConf conf = new SparkConf().setAppName("spark-infinispan-stream-consumer-java");

      // Create the streaming context
      JavaStreamingContext javaStreamingContext = new JavaStreamingContext(conf, Seconds.apply(1));

      // Populate infinispan properties
      Properties infinispanProperties = new Properties();
      infinispanProperties.put("infinispan.client.hotrod.server_list", infinispanHost);

      JavaReceiverInputDStream<Status> twitterDStream = TwitterUtils.createStream(javaStreamingContext);

      // Transform from twitter4j.Status to our domain model org.infinispan.spark.demo.twitter.Tweet
      JavaDStream<Tuple2<Long, Tweet>> kvPair = twitterDStream.map(status -> new Tuple2<>(status.getId(), new Tweet(status.getId(),
              status.getUser().getScreenName(),
              Optional.ofNullable(status.getPlace()).map(Place::getCountry).orElseGet(() -> "N/A"),
              status.getRetweetCount(),
              status.getText())));

      // Write the stream to infinispan
      InfinispanJavaDStream.writeToInfinispan(kvPair, infinispanProperties);

      // Print cache status every 5 seconds
      CacheStatus cacheStatus = new CacheStatus(infinispanHost);
      cacheStatus.printStatus(5, TimeUnit.SECONDS);

      // Start the processing
      javaStreamingContext.start();

      javaStreamingContext.awaitTermination();
   }

   private static class CacheStatus {

      private final RemoteCache<Long, Tweet> cache;

      public CacheStatus(String master) {
         Configuration configuration = new ConfigurationBuilder().addServers(master).pingOnStartup(true).create();
         cache = new RemoteCacheManager(configuration).getCache();
      }

      public void printStatus(long value, TimeUnit timeUnit) {
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
