package org.infinispan.spark.suites

import org.infinispan.spark.domain.Runner
import org.infinispan.spark.test._
import org.scalatest.{DoNotDiscover, FunSuite, Matchers}

@DoNotDiscover
class CustomFilterRDDSuite extends FunSuite with RunnersCache with Spark with MultipleServers with Matchers {
   override protected def getNumEntries: Int = 100

   override def getCacheConfig: Option[String] = Some("""{"replicated-cache":{"mode":"SYNC"}}""")

   test("Filter by deployed server-side filter") {
      val rdd = createInfinispanRDD[Int, Runner].filterByCustom[String]("sample-filter-factory")
      val results = rdd.values

      rdd.count shouldBe getNumEntries / 2

      all(results.collect()) should fullyMatch regex """Runner \d*""".r
   }

   test("Filter by deployed server-side filter with param") {
      val rdd = createInfinispanRDD[Int, Runner].filterByCustom[String]("sample-filter-factory-with-param", 5: java.lang.Integer)
      val results = rdd.values

      rdd.count shouldBe getNumEntries

      all(results.collect()) shouldBe "Runne"
   }

}