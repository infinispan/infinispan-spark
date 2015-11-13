package org.infinispan.spark.suites

import org.infinispan.client.hotrod.configuration.ConfigurationBuilder
import org.infinispan.client.hotrod.impl.query.RemoteQuery
import org.infinispan.client.hotrod.marshall.ProtoStreamMarshaller
import org.infinispan.client.hotrod.{RemoteCacheManager, Search}
import org.infinispan.protostream.annotations.ProtoSchemaBuilder
import org.infinispan.query.remote.client.ProtobufMetadataManagerConstants
import org.infinispan.spark.domain._
import org.infinispan.spark.test._
import org.scalatest.{DoNotDiscover, FunSuite, Matchers}


@DoNotDiscover
class FilterByQueryProtoAnnotationSuite extends FunSuite with RunnersCache with Spark with MultipleServers with Matchers {
   override protected def getNumEntries: Int = 100

   override def getCacheType: CacheType.Value = CacheType.REPLICATED

   override lazy val remoteCacheManager: RemoteCacheManager = {
      val rcm = new RemoteCacheManager(
         new ConfigurationBuilder().addServer().host("localhost").port(getServerPort).marshaller(new ProtoStreamMarshaller).build()
      )
      val serializationContext = ProtoStreamMarshaller.getSerializationContext(rcm)
      val protoSchemaBuilder = new ProtoSchemaBuilder
      val protoFile = protoSchemaBuilder.fileName("runner.proto").addClass(classOf[Runner]).build(serializationContext)
      rcm.getCache(ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME).put("runner.proto", protoFile)
      rcm
   }

   test("Filter by Query") {
      val query = Search.getQueryFactory(remoteCacheManager.getCache(getCacheName)).from(classOf[Runner]).having("finishTimeSeconds")
            .between(4000, 4500).toBuilder[RemoteQuery].build

      val rdd = createInfinispanRDD[Int, Runner].filterByQuery[Runner](query, classOf[Runner])

      rdd.count shouldBe query.getResultSize

      rdd.first()._2.getFinishTimeSeconds should be(4000 +- 4500)
   }

   test("Filter by Query with projections") {
      val query = Search.getQueryFactory(remoteCacheManager.getCache(getCacheName)).from(classOf[Runner]).select("name", "age").having("finished").equal(true)
            .toBuilder[RemoteQuery].build()

      val rdd = createInfinispanRDD[Int, Runner].filterByQuery[Array[AnyRef]](query, classOf[Runner])
      val first = rdd.values.collect().head

      first(0).getClass shouldBe classOf[String]
      first(1).getClass shouldBe classOf[Integer]

      rdd.count shouldBe query.getResultSize
   }

}

