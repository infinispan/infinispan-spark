package org.infinispan.spark.examples.twitter;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import static org.apache.spark.storage.StorageLevel.MEMORY_ONLY;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.infinispan.client.hotrod.event.ClientEvent;
import static org.infinispan.spark.examples.twitter.Sample.runAndExit;
import static org.infinispan.spark.examples.twitter.Sample.usage;
import org.infinispan.spark.examples.util.TwitterDStream;
import org.infinispan.spark.stream.InfinispanJavaDStream;
import scala.Tuple2;
import scala.Tuple3;

import java.util.List;
import java.util.Properties;

/**
 * This demo will start a DStream from Twitter and will save it to Infinispan after applying a transformation. At the
 * same time, it will listen to {@link Tweet} insertions and print a summary by country in the last 60 seconds.
 *
 * @author gustavonalle
 */
public class StreamProducerJava {

   public static void main(String[] args) {
      if (args.length < 2) {
         usage(StreamProducerJava.class.getSimpleName());
      }

      String infinispanHost = args[0];
      Long duration = Long.parseLong(args[1]) * 1000;

      // Reduce the log level in the driver
      Logger.getLogger("org").setLevel(Level.WARN);

      SparkConf conf = Sample.getSparkConf("spark-infinispan-stream-producer-java");

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

      // Create InfinispanInputDStream
      JavaInputDStream<Tuple3<Long, Tweet, ClientEvent.Type>> infinispanInputDStream =
              InfinispanJavaDStream.createInfinispanInputDStream(javaStreamingContext, MEMORY_ONLY(), infinispanProperties);

      // Apply a transformation to the RDDs to aggregate by country
      JavaPairDStream<String, Integer> countryDStream = infinispanInputDStream.transformToPair(rdd -> {
         return rdd.filter(ev -> !ev._2().getCountry().equals("N/A"))
                 .mapToPair(event -> new Tuple2<>(event._2().getCountry(), 1))
                 .reduceByKey((a, b) -> a + b);
      });

      //Since we are interested in the last 60 seconds only, we restrict the DStream by window, collapsing all the RDDs:
      JavaPairDStream<String, Integer> lastMinuteStream = countryDStream.reduceByKeyAndWindow((a, b) -> a + b, new Duration(60 * 1000));

      lastMinuteStream.foreachRDD((rdd, time) -> {
         System.out.format("---------- %s ----------\n", time.toString());
         List<Tuple2<String, Integer>> results = rdd.collect();
         results.stream().sorted((o1, o2) -> o2._2().compareTo(o1._2())).forEach(t -> System.out.format("[%s,%d]\n", t._1(), t._2()));
      });

      // Start the processing
      runAndExit(javaStreamingContext.ssc(), duration);
   }

}
