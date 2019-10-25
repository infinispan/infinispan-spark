package org.infinispan.spark.suites

import org.infinispan.client.hotrod.Search
import org.infinispan.spark.domain._
import org.infinispan.spark.test._
import org.scalatest.{DoNotDiscover, FunSuite, Matchers}


@DoNotDiscover
class FilterByQueryProtoAnnotationSuite extends FunSuite with RunnersCache with Spark with MultipleServers with Matchers {
   override protected def getNumEntries: Int = 100

   override def getCacheConfig: Option[String] = Some("""{"replicated-cache":{"mode":"SYNC"}}""")

   override def getConfiguration = {
      val configuration = super.getConfiguration
      configuration.addProtoAnnotatedClass(classOf[Runner])
      configuration.setAutoRegisterProto()
      configuration
   }

   test("Filter by Query") {
      val query = Search.getQueryFactory(remoteCacheManager.getCache(getCacheName)).from(classOf[Runner])
        .having("finishTimeSeconds").between(4000, 4500).build

      val rdd = createInfinispanRDD[Int, Runner].filterByQuery[Runner](query)

      rdd.count shouldBe query.getResultSize

      rdd.first()._2.getFinishTimeSeconds should be(4000 +- 4500)
   }

   test("Filter by Query String") {
      val ickleQuery = "FROM runner WHERE finishTimeSeconds BETWEEN 4000 AND 4500"

      val rdd = createInfinispanRDD[Int, Runner].filterByQuery[Runner](ickleQuery)

      rdd.count shouldBe Search.getQueryFactory(remoteCacheManager.getCache(getCacheName)).create(ickleQuery).list().size()

      rdd.first()._2.getFinishTimeSeconds should be(4000 +- 4500)
   }

   test("Filter by Query with projections") {
      val query = Search.getQueryFactory(remoteCacheManager.getCache(getCacheName)).from(classOf[Runner]).select("name", "age").having("finished").equal(true)
        .build()

      val rdd = createInfinispanRDD[Int, Runner].filterByQuery[Array[AnyRef]](query)
      val first = rdd.values.collect().head

      first(0).getClass shouldBe classOf[String]
      first(1).getClass shouldBe classOf[Integer]

      rdd.count shouldBe query.getResultSize
   }

}

