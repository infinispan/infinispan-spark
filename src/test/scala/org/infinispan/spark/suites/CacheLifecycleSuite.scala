package org.infinispan.spark.suites

import org.infinispan.spark._
import org.infinispan.spark.domain.Runner
import org.infinispan.spark.rdd.InfinispanRDD
import org.infinispan.spark.test.{MultipleServers, RunnersCache, Spark}
import org.scalatest.{DoNotDiscover, FunSuite, Matchers}

@DoNotDiscover
class CacheLifecycleSuite extends FunSuite with RunnersCache with Spark with MultipleServers with Matchers {

   override protected def getNumEntries: Int = 100

   test("Reuse same RDD") {
      val infinispanRDD = createInfinispanRDD[Int, Runner]

      val younger = infinispanRDD.sortByKey().values.first()
      younger.getAge shouldBe 15

      val older = infinispanRDD.sortByKey(ascending = false).values.first()
      older.getAge shouldBe 60
   }

   test("caches lifecycle") {
      val infinispanRDD = createInfinispanRDD[Int, Runner]

      val cacheAdmin = infinispanRDD.cacheAdmin()

      val rddCache = infinispanRDD.configuration.getCacheName
      cacheAdmin.exists(rddCache) shouldBe true
      cacheAdmin.exists("invalid_cache_name") shouldBe false

      infinispanRDD.count() shouldBe getNumEntries
      cacheAdmin.clear(rddCache)
      infinispanRDD.count() shouldBe 0

      val cacheConfig =
         <infinispan>
            <cache-container>
               <distributed-cache name="tempCache"/>
            </cache-container>
         </infinispan>

      cacheAdmin.exists("tempCache") shouldBe false
      cacheAdmin.createFromConfig("tempCache", cacheConfig.toString())
      cacheAdmin.exists("tempCache") shouldBe true

      val configuration = getConfiguration.setCacheName("tempCache")
      new InfinispanRDD[String, String](sc, configuration = configuration).count() shouldBe 0

      sc.parallelize((0 until 20)
         .map(new Runner("name", true, _, 40)))
         .zipWithIndex().map(_.swap)
         .writeToInfinispan(configuration)

      new InfinispanRDD[String, String](sc, configuration = configuration).count() shouldBe 20

      cacheAdmin.delete("tempCache")
      cacheAdmin.exists("tempCache") shouldBe false

      cacheAdmin.exists("from-template") shouldBe false
      cacheAdmin.createFromTemplate("from-template", "replicated")
      cacheAdmin.exists("from-template") shouldBe true

   }

   test("Auto create caches from config") {
      val cacheCfg =
         <infinispan>
            <cache-container>
               <replicated-cache name="myCache">
                  <indexing index="NONE"/>
               </replicated-cache>
            </cache-container>
         </infinispan>

      val cfg = getConfiguration.setCacheName("myCache").setAutoCreateCacheFromConfig(cacheCfg.toString())

      val rdd = new InfinispanRDD[String, String](sc, configuration = cfg)

      rdd.values.count() shouldBe 0
   }

   test("Auto create caches from template") {
      val cfg = getConfiguration.setCacheName("pokemon").setAutoCreateCacheFromTemplate("replicated")

      val rdd = new InfinispanRDD[String, String](sc, configuration = cfg)

      rdd.values.count() shouldBe 0
   }

}
