package org.infinispan.spark.suites

import java.util.concurrent.TimeUnit

import org.apache.spark.storage.StorageLevel
import org.infinispan.client.hotrod.RemoteCache
import org.infinispan.client.hotrod.event.ClientEvent
import org.infinispan.client.hotrod.event.ClientEvent.Type.{CLIENT_CACHE_ENTRY_CREATED, CLIENT_CACHE_ENTRY_EXPIRED, CLIENT_CACHE_ENTRY_MODIFIED, CLIENT_CACHE_ENTRY_REMOVED}
import org.infinispan.spark.config.ConnectorConfiguration
import org.infinispan.spark.domain.Runner
import org.infinispan.spark.stream._
import org.infinispan.spark.test.StreamingUtils.TestInputDStream
import org.infinispan.spark.test.TestingUtil._
import org.infinispan.spark.test.{CacheType, MultipleServers, SparkStream}
import org.jboss.dmr.scala.ModelNode
import org.scalatest.{DoNotDiscover, FunSuite, Matchers}

import scala.collection._
import scala.concurrent.duration._
import scala.language.postfixOps

/**
 * @author gustavonalle
 */
@DoNotDiscover
class StreamingSuite extends FunSuite with SparkStream with MultipleServers with Matchers {

   override def getCacheConfig: Option[ModelNode] = Some(ModelNode(
      "expiration" -> ModelNode(
         "EXPIRATION" -> ModelNode(
            "interval" -> 500
         )
      )
   ))

   private def getProperties = {
      new ConnectorConfiguration()
        .setServerList(s"localhost:$getServerPort")
        .setCacheName(getCacheName)
   }

   test("test stream consumer") {
      val cache = getRemoteCache.asInstanceOf[RemoteCache[Int, String]]
      val stream = new TestInputDStream(ssc, of = Seq(1 -> "value1", 2 -> "value2", 3 -> "value3"), streamItemEvery = 100 millis)

      stream.writeToInfinispan(getProperties)

      ssc.start()

      waitForCondition(() => cache.size == 3)
      cache.get(1) shouldBe "value1"
      cache.get(2) shouldBe "value2"
      cache.get(3) shouldBe "value3"
   }

   test("test stream producer") {
      val cache = getRemoteCache.asInstanceOf[RemoteCache[Int, Runner]]
      cache.clear()

      val stream = new InfinispanInputDStream[Int, Runner](ssc, StorageLevel.MEMORY_ONLY, getProperties)
      val streamDump = mutable.Set[(Int, Runner, ClientEvent.Type)]()

      stream.foreachRDD(rdd => streamDump ++= rdd.collect())

      ssc.start()

      executeAfterReceiverStarted {
         cache.put(1, new Runner("Bolt", finished = true, 3600, 30))
         cache.put(2, new Runner("Farah", finished = true, 7200, 29))
         cache.put(3, new Runner("Ennis", finished = true, 7500, 28))
         cache.put(4, new Runner("Gatlin", finished = true, 7900, 26), 50, TimeUnit.MILLISECONDS)
         cache.put(1, new Runner("Bolt", finished = true, 7500, 23))
         cache.remove(2)
      }

      waitForCondition(() => streamDump.size == 7)
      eventsOfType(streamDump)(CLIENT_CACHE_ENTRY_CREATED) shouldBe 4
      eventsOfType(streamDump)(CLIENT_CACHE_ENTRY_REMOVED) shouldBe 1
      eventsOfType(streamDump)(CLIENT_CACHE_ENTRY_MODIFIED) shouldBe 1
      eventsOfType(streamDump)(CLIENT_CACHE_ENTRY_EXPIRED) shouldBe 1
   }

   test("test stream with current state") {
      val cache = getRemoteCache.asInstanceOf[RemoteCache[Int, String]]
      cache.clear()

      (0 until 50).foreach(n => cache.put(n, s"value$n"))

      val stream = new InfinispanInputDStream[Int, Runner](ssc, StorageLevel.MEMORY_ONLY, getProperties, includeState = true)

      val streamDump = mutable.Set[(Int, Runner, ClientEvent.Type)]()

      stream.foreachRDD(rdd => streamDump ++= rdd.collect())
      ssc.start()

      waitForCondition(() => streamDump.size == 50)
   }

   private def eventsOfType(streamDump: Set[(Int, Runner, ClientEvent.Type)])(eventType: ClientEvent.Type): Int = {
      streamDump.count { case (_, _, t) => t == eventType }
   }

   override def getCacheType: CacheType.Value = CacheType.DISTRIBUTED
}
