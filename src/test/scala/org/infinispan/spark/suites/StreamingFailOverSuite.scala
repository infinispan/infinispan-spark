package org.infinispan.spark.suites

import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.spark.storage.StorageLevel
import org.infinispan.client.hotrod.RemoteCache
import org.infinispan.client.hotrod.event.ClientEvent
import org.infinispan.client.hotrod.event.ClientEvent.Type.{CLIENT_CACHE_ENTRY_CREATED, CLIENT_CACHE_ENTRY_EXPIRED, CLIENT_CACHE_ENTRY_MODIFIED, CLIENT_CACHE_ENTRY_REMOVED}
import org.infinispan.spark.domain.Runner
import org.infinispan.spark.stream._
import org.infinispan.spark.test.StreamingUtils.TestInputDStream
import org.infinispan.spark.test.TestingUtil._
import org.infinispan.spark.test._
import org.jboss.dmr.scala.ModelNode
import org.scalatest.{DoNotDiscover, FunSuite, Matchers}

import scala.collection._
import scala.concurrent.duration._
import scala.language.postfixOps

@DoNotDiscover
class StreamingFailOverSuite extends FunSuite with SparkStream with MultipleServers with FailOver with Matchers {

   override def getCacheConfig: Option[ModelNode] = Some(ModelNode(
      "expiration" -> ModelNode(
         "EXPIRATION" -> ModelNode(
            "interval" -> 500
         )
      )
   ))

   protected def getProperties = {
      val props = new Properties()
      props.put("infinispan.client.hotrod.server_list", s"localhost:$getServerPort")
      props.put("infinispan.rdd.cacheName", getCacheName)
      props
   }

   override def getCacheType: CacheType.Value = CacheType.DISTRIBUTED

   test("test stream consumer with failover") {
      val cache = getRemoteCache.asInstanceOf[RemoteCache[Int, String]]
      val stream = new TestInputDStream(ssc, of = Seq(1 -> "value1", 2 -> "value2", 3 -> "value3"), streamItemEvery = 100 millis)

      stream.writeToInfinispan(getProperties)

      ssc.start()

      Thread.sleep(100)
      Cluster.failServer(0)

      waitForCondition(() => cache.size == 3)
      cache.get(1) shouldBe "value1"
      cache.get(2) shouldBe "value2"
      cache.get(3) shouldBe "value3"
   }

   test("test stream producer with failover") {
      val cache = getRemoteCache.asInstanceOf[RemoteCache[Int, Runner]]
      cache.clear()

      val stream = new InfinispanInputDStream[Int, Runner](ssc, StorageLevel.MEMORY_ONLY, getProperties)
      val streamDump = mutable.Set[(Int, Runner, ClientEvent.Type)]()

      stream.foreachRDD(rdd => streamDump ++= rdd.collect())

      ssc.start()

      executeAfterReceiverStarted {
         cache.put(1, new Runner("Bolt", finished = true, 3600, 30))
         cache.put(2, new Runner("Farah", finished = true, 7200, 29))

         Cluster.failServer(0)

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

   protected def eventsOfType(streamDump: Set[(Int, Runner, ClientEvent.Type)])(eventType: ClientEvent.Type): Int = {
      streamDump.count { case (_, _, t) => t == eventType }
   }
}