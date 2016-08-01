package org.infinispan.spark.suites

import java.util
import java.util.Properties

import org.apache.spark.SparkConf
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder
import org.infinispan.client.hotrod.marshall.ProtoStreamMarshaller
import org.infinispan.client.hotrod.{RemoteCacheManager, Search}
import org.infinispan.protostream.FileDescriptorSource
import org.infinispan.query.remote.client.ProtobufMetadataManagerConstants
import org.infinispan.spark.domain._
import org.infinispan.spark.rdd.InfinispanRDD
import org.infinispan.spark.test._
import org.scalatest.{DoNotDiscover, FunSuite, Matchers}

@DoNotDiscover
class FilterByQueryProtoSuite extends FunSuite with Spark with MultipleServers with Matchers {

   override def getCacheType: CacheType.Value = CacheType.DISTRIBUTED

   override def getSparkConfig: SparkConf = {
      val config = super.getSparkConfig
      config.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      config
   }

   override lazy val remoteCacheManager: RemoteCacheManager = {
      val rcm = new RemoteCacheManager(
         new ConfigurationBuilder().addServer().host("localhost").port(getServerPort).marshaller(new ProtoStreamMarshaller).build()
      )
      rcm.getCache(ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME).put("test.proto", protoFile)

      val serCtx = ProtoStreamMarshaller.getSerializationContext(rcm)
      serCtx.registerProtoFiles(FileDescriptorSource.fromString("test.proto", protoFile))
      serCtx.registerMarshaller(new AddressMarshaller)
      serCtx.registerMarshaller(new PersonMarshaller)
      rcm
   }


   val protoFile =
      """
      package org.infinispan.spark.domain;

      message Person {
         required string name = 1;
         optional int32 age = 2;
         optional Address address = 3;
      }

      message Address {
         required string street = 1;
         required int32 number = 2;
         required string country = 3;
      }

      """.stripMargin


   val protoConfig = {
      val map = new util.HashMap[String, String]()
      map.put("test.proto", protoFile)
      map
   }


   test("Filter by Query with proto file and provided marshallers") {

      val defaultCache = remoteCacheManager.getCache[Int, Person]
      (1 to 20).foreach { idx =>
         defaultCache.put(idx, new Person(s"name$idx", idx, new Address(s"street$idx", idx, "N/A")));
      }

      val configuration = new Properties

      configuration.put(InfinispanRDD.ProtoFiles, protoConfig)
      configuration.put(InfinispanRDD.Marshallers, Seq(classOf[AddressMarshaller], classOf[PersonMarshaller]))
      configuration.put("infinispan.client.hotrod.server_list", Seq("localhost", getServerPort).mkString(":"))


      val rdd = new InfinispanRDD[Int, Person](sc, configuration)

      val query = Search.getQueryFactory(defaultCache)
              .from(classOf[Person]).having("address.number").gt(10)
              .toBuilder.build()

      val filteredRdd = rdd.filterByQuery[Person](query, classOf[Person])

      val result = filteredRdd.values.collect()

      result.length shouldBe 10
      result.sortWith(_.getName > _.getName).head.getName shouldBe "name20"
   }

}
