package org.infinispan.spark;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.scheduler.StreamingListener;
import org.apache.spark.streaming.scheduler.StreamingListenerBatchCompleted;
import org.apache.spark.streaming.scheduler.StreamingListenerBatchStarted;
import org.apache.spark.streaming.scheduler.StreamingListenerBatchSubmitted;
import org.apache.spark.streaming.scheduler.StreamingListenerReceiverError;
import org.apache.spark.streaming.scheduler.StreamingListenerReceiverStarted;
import org.apache.spark.streaming.scheduler.StreamingListenerReceiverStopped;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.event.ClientEvent;
import org.infinispan.spark.domain.Person;
import org.infinispan.spark.stream.InfinispanJavaDStream;
import scala.Tuple2;
import scala.Tuple3;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import static org.infinispan.spark.test.StreamingUtils.createJavaReceiverDInputStream;
import static org.junit.Assert.assertEquals;

public class JavaStreamApiTest {

   public void testStreamConsumer(JavaStreamingContext jssc, Properties config, RemoteCache<Integer, Person> cache) {
      List<Tuple2<Integer, Person>> data = Arrays.asList(
              new Tuple2<>(1, createPerson(1)),
              new Tuple2<>(2, createPerson(2)),
              new Tuple2<>(3, createPerson(3))
      );

      JavaReceiverInputDStream<Tuple2<Integer, Person>> dstream =
              createJavaReceiverDInputStream(jssc, data, Duration.ofMillis(100));

      InfinispanJavaDStream.writeToInfinispan(dstream, config);

      jssc.start();
      jssc.awaitTerminationOrTimeout(2000);

      assertEquals(3, cache.size());
      assertEquals("name1", cache.get(1).getName());
      assertEquals("name2", cache.get(2).getName());
      assertEquals("name3", cache.get(3).getName());
   }

   public void testStreamProducer(JavaStreamingContext jssc, Properties config, RemoteCache<Integer, Person> cache) {
      JavaInputDStream<Tuple3<Integer, Person, ClientEvent.Type>> inputDStream =
              InfinispanJavaDStream.<Integer, Person>createInfinispanInputDStream(jssc, StorageLevel.MEMORY_ONLY(), config);

      Set<Tuple3<Integer, Person, ClientEvent.Type>> streamDump = new HashSet<>();

      inputDStream.foreachRDD((v1, v2) -> {
         streamDump.addAll(v1.collect());
         return null;
      });

      jssc.start();

      jssc.addStreamingListener((ReceiverStartListener) receiverStarted -> {
         cache.put(1, createPerson(1));
         cache.put(2, createPerson(2));
         cache.put(3, createPerson(3));
         cache.put(1, createPerson(11));
         cache.remove(2);
      });

      jssc.awaitTerminationOrTimeout(2000);

      assertEquals(5, streamDump.size());
      assertEquals(3, streamDump.stream().filter(p -> p._3() == ClientEvent.Type.CLIENT_CACHE_ENTRY_CREATED).count());
      assertEquals(1, streamDump.stream().filter(p -> p._3() == ClientEvent.Type.CLIENT_CACHE_ENTRY_REMOVED).count());
      assertEquals(1, streamDump.stream().filter(p -> p._3() == ClientEvent.Type.CLIENT_CACHE_ENTRY_MODIFIED).count());
   }

   private Person createPerson(int seed) {
      return new Person("name" + seed, seed, null);
   }

   private interface ReceiverStartListener extends StreamingListener {
      @Override
      void onReceiverStarted(StreamingListenerReceiverStarted receiverStarted);

      @Override
      default void onReceiverError(StreamingListenerReceiverError receiverError) {
      }

      @Override
      default void onReceiverStopped(StreamingListenerReceiverStopped receiverStopped) {
      }

      @Override
      default void onBatchSubmitted(StreamingListenerBatchSubmitted batchSubmitted) {
      }

      @Override
      default void onBatchStarted(StreamingListenerBatchStarted batchStarted) {
      }

      @Override
      default void onBatchCompleted(StreamingListenerBatchCompleted batchCompleted) {
      }
   }

}
