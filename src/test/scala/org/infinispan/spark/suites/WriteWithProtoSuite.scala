package org.infinispan.spark.suites

import java.util.Properties

import org.infinispan.client.hotrod.configuration.ConfigurationBuilder
import org.infinispan.client.hotrod.marshall.ProtoStreamMarshaller
import org.infinispan.client.hotrod.{RemoteCache, RemoteCacheManager, Search}
import org.infinispan.protostream.annotations.ProtoSchemaBuilder
import org.infinispan.spark._
import org.infinispan.spark.domain.Runner
import org.infinispan.spark.rdd.InfinispanRDD
import org.infinispan.spark.test.{SingleStandardServer, Spark}
import org.scalatest.{DoNotDiscover, FunSuite, Matchers}

import java.lang.Boolean._

@DoNotDiscover
class WriteWithProtoSuite extends FunSuite with Spark with SingleStandardServer with Matchers {

   override lazy val remoteCacheManager: RemoteCacheManager = {
      val rcm = new RemoteCacheManager(
         new ConfigurationBuilder().addServer().host("localhost").port(getServerPort).marshaller(new ProtoStreamMarshaller).build()
      )
      val serializationContext = ProtoStreamMarshaller.getSerializationContext(rcm)
      new ProtoSchemaBuilder().fileName("runner.proto").addClass(classOf[Runner]).build(serializationContext)
      rcm
   }


   test("write proto annotated entity to Infinispan with auto registration") {
      val protoAnnotatedEntities = for (num <- 0 to 999) yield new Runner(s"name$num", true, num * 10, (1000 - 30) / 50)
      val memoryRDD = sc.parallelize(protoAnnotatedEntities).zipWithIndex().map(_.swap)
      memoryRDD.writeToInfinispan(getConfiguration)

      val cache = getRemoteCache.asInstanceOf[RemoteCache[Int, Runner]]
      val query = Search.getQueryFactory(cache).create("FROM runner WHERE name = 'name121'")

      val runners = query.list[Runner]()
      runners.size shouldBe 1
   }

   override def getConfiguration: Properties = {
      val config = super.getConfiguration
      config.put(InfinispanRDD.ProtoEntities, Seq(classOf[Runner]))
      config.put(InfinispanRDD.RegisterSchemas, TRUE)
      config
   }
}