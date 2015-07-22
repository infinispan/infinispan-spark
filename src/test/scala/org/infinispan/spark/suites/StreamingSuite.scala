package org.infinispan.spark.suites

import java.util.Properties

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.scheduler.{StreamingListener, StreamingListenerReceiverStarted}
import org.infinispan.client.hotrod.RemoteCache
import org.infinispan.client.hotrod.event.ClientEvent
import org.infinispan.client.hotrod.event.ClientEvent.Type.{CLIENT_CACHE_ENTRY_CREATED, CLIENT_CACHE_ENTRY_MODIFIED, CLIENT_CACHE_ENTRY_REMOVED}
import org.infinispan.spark.domain.Runner
import org.infinispan.spark.stream._
import org.infinispan.spark.test.StreamingUtils.TestInputDStream
import org.infinispan.spark.test.{CacheType, MultipleServers, SparkStream}
import org.scalatest.{DoNotDiscover, FunSuite, Matchers}

import scala.collection._
import scala.concurrent.duration._
import scala.language.postfixOps

/**
 * @author gustavonalle
 */
@DoNotDiscover
class StreamingSuite extends FunSuite with SparkStream with MultipleServers with Matchers {

   override def getCacheName: String = "streaming-cache"

   private def getProperties = {
      val props = new Properties()
      props.put("infinispan.client.hotrod.server_list", s"localhost:$getServerPort")
      props.put("infinispan.rdd.cacheName", getCacheName)
      props
   }

   test("test stream consumer") {
      val cache = getTargetCache.asInstanceOf[RemoteCache[Int, String]]
      val stream = new TestInputDStream(ssc, of = Seq(1 -> "value1", 2 -> "value2", 3 -> "value3"), streamItemEvery = 100 millis)

      stream.writeToInfinispan(getProperties)

      ssc.start()
      ssc.awaitTerminationOrTimeout(2000)

      cache.size shouldBe 3
      cache.get(1) shouldBe "value1"
      cache.get(2) shouldBe "value2"
      cache.get(3) shouldBe "value3"
   }

   test("test stream producer") {
      val cache = getTargetCache.asInstanceOf[RemoteCache[Int, Runner]]
      cache.clear()

      val stream = new InfinispanInputDStream[Int, Runner](ssc, StorageLevel.MEMORY_ONLY, getProperties)
      val streamDump = mutable.Set[(Int, Runner, ClientEvent.Type)]()

      stream.foreachRDD(rdd => streamDump ++= rdd.collect())

      ssc.start()

      ssc.addStreamingListener(new StreamingListener {
         override def onReceiverStarted(receiverStarted: StreamingListenerReceiverStarted): Unit = {
            cache.put(1, new Runner("Bolt", finished = true, 3600, 30))
            cache.put(2, new Runner("Farah", finished = true, 7200, 29))
            cache.put(3, new Runner("Ennis", finished = true, 7500, 28))
            cache.put(1, new Runner("Bolt", finished = true, 7500, 23))
            cache.remove(2)
         }
      })

      ssc.awaitTerminationOrTimeout(2000)

      streamDump.size shouldBe 5
      streamDump.count { case (_, _, eventType) => eventType == CLIENT_CACHE_ENTRY_CREATED } shouldBe 3
      streamDump.count { case (_, _, eventType) => eventType == CLIENT_CACHE_ENTRY_REMOVED } shouldBe 1
      streamDump.count { case (_, _, eventType) => eventType == CLIENT_CACHE_ENTRY_MODIFIED } shouldBe 1
   }

   override def getCacheType: CacheType.Value = CacheType.DISTRIBUTED
}
