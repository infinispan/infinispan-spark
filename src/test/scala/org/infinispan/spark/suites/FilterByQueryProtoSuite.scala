package org.infinispan.spark.suites

import org.apache.spark.SparkConf
import org.infinispan.client.hotrod.{RemoteCache, Search}
import org.infinispan.spark.config.ConnectorConfiguration
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

   override def getConfiguration: ConnectorConfiguration = {
      val cfg = super.getConfiguration
      cfg.addProtoFile("test.proto", protoFile)
        .setAutoRegisterProto()
        .addMessageMarshaller(classOf[AddressMarshaller])
        .addMessageMarshaller(classOf[PersonMarshaller])
   }

   lazy val remoteCache: RemoteCache[Int, Person] = {
      val cache = remoteCacheManager.getCache[Int, Person](getCacheName)
      (1 to 20).foreach { idx =>
         cache.put(idx, new Person(s"name$idx", idx, new Address(s"street$idx", idx, "N/A")));
      }
      cache
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

   test("Filter by Query with proto file and provided marshallers") {
      val rdd = new InfinispanRDD[Int, Person](sc, getConfiguration)

      val query = Search.getQueryFactory(remoteCache).from(classOf[Person]).having("address.number").gt(10).build()

      val filteredRdd = rdd.filterByQuery[Person](query)

      val result = filteredRdd.values.collect()

      result.length shouldBe 10
      result.sortWith(_.getName > _.getName).head.getName shouldBe "name20"
   }

   test("Filter by Query String") {
      val rdd = new InfinispanRDD[Int, Person](sc, getConfiguration)

      val filteredRdd = rdd.filterByQuery[Person]("From org.infinispan.spark.domain.Person p where p.address.number > 10")

      val result = filteredRdd.values.collect()

      result.length shouldBe 10
      result.sortWith(_.getName > _.getName).head.getName shouldBe "name20"
   }

}
