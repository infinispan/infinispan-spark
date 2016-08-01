package org.infinispan.spark;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.scheduler.StreamingListener;
import org.apache.spark.streaming.scheduler.StreamingListenerBatchCompleted;
import org.apache.spark.streaming.scheduler.StreamingListenerBatchStarted;
import org.apache.spark.streaming.scheduler.StreamingListenerBatchSubmitted;
import org.apache.spark.streaming.scheduler.StreamingListenerOutputOperationCompleted;
import org.apache.spark.streaming.scheduler.StreamingListenerOutputOperationStarted;
import org.apache.spark.streaming.scheduler.StreamingListenerReceiverError;
import org.apache.spark.streaming.scheduler.StreamingListenerReceiverStarted;
import org.apache.spark.streaming.scheduler.StreamingListenerReceiverStopped;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.event.ClientEvent;
import org.infinispan.spark.domain.Person;
import org.infinispan.spark.stream.InfinispanJavaDStream;
import static org.infinispan.spark.test.StreamingUtils.createJavaReceiverDInputStream;
import org.infinispan.spark.test.TestingUtil;
import static org.junit.Assert.assertEquals;
import scala.Tuple2;
import scala.Tuple3;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

public class JavaStreamApiTest {

   public void testStreamConsumer(JavaStreamingContext jssc, Properties config, RemoteCache<Integer, Person> cache) throws InterruptedException {
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

      inputDStream.foreachRDD((v1, time) -> streamDump.addAll(v1.collect()));

      jssc.start();

      executeAfterReceiverStarted(jssc, () -> {
         cache.put(1, createPerson(1));
         cache.put(2, createPerson(2));
         cache.put(3, createPerson(3));
         cache.put(1, createPerson(11));
         cache.remove(2);
      });

      TestingUtil.waitForCondition(() -> streamDump.size() == 5);
      assertEquals(3, streamDump.stream().filter(p -> p._3() == ClientEvent.Type.CLIENT_CACHE_ENTRY_CREATED).count());
      assertEquals(1, streamDump.stream().filter(p -> p._3() == ClientEvent.Type.CLIENT_CACHE_ENTRY_REMOVED).count());
      assertEquals(1, streamDump.stream().filter(p -> p._3() == ClientEvent.Type.CLIENT_CACHE_ENTRY_MODIFIED).count());
   }

   private Person createPerson(int seed) {
      return new Person("name" + seed, seed, null);
   }

   private class ReceiverStartListener implements StreamingListener {
      @Override
      public void onReceiverStarted(StreamingListenerReceiverStarted receiverStarted) {
      }

      @Override
      public void onReceiverError(StreamingListenerReceiverError receiverError) {
      }

      @Override
      public void onReceiverStopped(StreamingListenerReceiverStopped receiverStopped) {
      }

      @Override
      public void onBatchSubmitted(StreamingListenerBatchSubmitted batchSubmitted) {
      }

      @Override
      public void onBatchStarted(StreamingListenerBatchStarted batchStarted) {
      }

      @Override
      public void onBatchCompleted(StreamingListenerBatchCompleted batchCompleted) {
      }

      @Override
      public void onOutputOperationStarted(StreamingListenerOutputOperationStarted outputOperationStarted) {
      }

      @Override
      public void onOutputOperationCompleted(StreamingListenerOutputOperationCompleted outputOperationCompleted) {
      }
   }

   private void executeAfterReceiverStarted(JavaStreamingContext context, Runnable code) {
      context.addStreamingListener(new ReceiverStartListener() {
         @Override
         public void onReceiverStarted(StreamingListenerReceiverStarted receiverStarted) {
            try {
               Thread.sleep(1000);
            } catch (InterruptedException ignored) {
            }
            code.run();
         }
      });
   }

}
