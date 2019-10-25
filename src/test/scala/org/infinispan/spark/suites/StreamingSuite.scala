package org.infinispan.spark.suites

import java.nio.charset.StandardCharsets.UTF_8
import java.util.concurrent.TimeUnit

import org.apache.spark.storage.StorageLevel
import org.infinispan.client.hotrod.RemoteCache
import org.infinispan.client.hotrod.event.ClientEvent
import org.infinispan.client.hotrod.event.ClientEvent.Type.{CLIENT_CACHE_ENTRY_CREATED, CLIENT_CACHE_ENTRY_EXPIRED, CLIENT_CACHE_ENTRY_MODIFIED, CLIENT_CACHE_ENTRY_REMOVED}
import org.infinispan.commons.dataconversion.MediaType
import org.infinispan.commons.dataconversion.MediaType.{APPLICATION_JSON, APPLICATION_JSON_TYPE}
import org.infinispan.commons.io.{ByteBuffer, ByteBufferImpl}
import org.infinispan.commons.marshall.AbstractMarshaller
import org.infinispan.spark.config.ConnectorConfiguration
import org.infinispan.spark.domain.Runner
import org.infinispan.spark.stream._
import org.infinispan.spark.test.StreamingUtils.TestInputDStream
import org.infinispan.spark.test.TestingUtil._
import org.infinispan.spark.test.{CacheType, MultipleServers, SparkStream}
import org.scalatest.{DoNotDiscover, FunSuite, Matchers}
import ujson.Js

import scala.collection._
import scala.concurrent.duration._
import scala.language.postfixOps

/**
 * @author gustavonalle
 */
@DoNotDiscover
class StreamingSuite extends FunSuite with SparkStream with MultipleServers with Matchers {

   override def getCacheConfig: Option[String] = Some(
      """
        |{
        |    "distributed-cache":{
        |        "mode":"SYNC",
        |        "owners":2,
        |        "statistics":true,
        |        "expiration":{
        |            "interval":500
        |        },
        |        "encoding":{
        |            "key":{
        |                "media-type":"application/x-jboss-marshalling"
        |            },
        |            "value":{
        |                "media-type":"application/x-jboss-marshalling"
        |            }
        |        }
        |    }
        |}
        |""".stripMargin
   )

   private def getProperties = {
      new ConnectorConfiguration()
        .setServerList(s"localhost:$getServerPort")
        .setCacheName(getCacheName)
   }

   private def getJsonProperties = {
      getProperties.setValueMarshaller(classOf[uJsonMarshaller]).setValueMediaType(APPLICATION_JSON_TYPE)
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
         cache.put(1, new Runner("Bolt", true, 3600, 30))
         cache.put(2, new Runner("Farah", true, 7200, 29))
         cache.put(3, new Runner("Ennis", true, 7500, 28))
         cache.put(4, new Runner("Gatlin", true, 7900, 26), 50, TimeUnit.MILLISECONDS)
         cache.put(1, new Runner("Bolt", true, 7500, 23))
         cache.remove(2)
      }

      waitForCondition(() => streamDump.size == 7)
      eventsOfType(streamDump)(CLIENT_CACHE_ENTRY_CREATED) shouldBe 4
      eventsOfType(streamDump)(CLIENT_CACHE_ENTRY_REMOVED) shouldBe 1
      eventsOfType(streamDump)(CLIENT_CACHE_ENTRY_MODIFIED) shouldBe 1
      eventsOfType(streamDump)(CLIENT_CACHE_ENTRY_EXPIRED) shouldBe 1
   }

   test("test stream in different format") {
      val cache = getRemoteCache.asInstanceOf[RemoteCache[Int, Runner]]
      cache.clear()

      val stream = new InfinispanInputDStream[Int, Js.Value](ssc, StorageLevel.MEMORY_ONLY, getJsonProperties)
      val streamDump = mutable.Set[(Int, Js.Value, ClientEvent.Type)]()

      stream.foreachRDD(rdd => streamDump ++= rdd.collect())

      ssc.start()

      executeAfterReceiverStarted {
         cache.put(1, new Runner("Bolt", true, 3600, 30))
         cache.put(2, new Runner("Farah", true, 7200, 29))
      }

      waitForCondition(() => streamDump.size == 2)

      extractValues(streamDump).forall(_ ("_type").str == classOf[Runner].getName)
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

   private def extractValues[T](streamDump: Set[(Int, T, ClientEvent.Type)]): Set[T] = streamDump.map { case (_, v, _) => v }

   override def getCacheType: CacheType.Value = CacheType.DISTRIBUTED
}

class uJsonMarshaller extends AbstractMarshaller {
   override def objectToBuffer(o: scala.Any, estimatedSize: Int): ByteBuffer = new ByteBufferImpl(strToByte(o.asInstanceOf[Js.Value].str))

   override def objectFromByteBuffer(buf: Array[Byte], offset: Int, length: Int): AnyRef = ujson.read(new String(buf))

   override def isMarshallable(o: scala.Any): Boolean = o.isInstanceOf[Js.Value]

   override def mediaType(): MediaType = APPLICATION_JSON

   private def strToByte(str: String) = str.getBytes(UTF_8)
}
