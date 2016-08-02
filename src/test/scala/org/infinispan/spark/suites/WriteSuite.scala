package org.infinispan.spark.suites

import org.infinispan.client.hotrod.RemoteCache
import org.infinispan.spark.domain.Runner
import org.infinispan.spark.test.{SingleSecureServer, SingleStandardServer, Spark}
import org.scalatest.{DoNotDiscover, FunSuite, Matchers}

@DoNotDiscover
class WriteSuite extends FunSuite with Spark with SingleStandardServer with Matchers {

   test("write to infinispan") {
      val entities = (for (num <- 0 to 999) yield new Runner(s"name$num", true, num * 10, (1000 - 30) / 50)).toSeq

      val rdd = sc.parallelize(entities).zipWithIndex().map(_.swap)

      val cache = getRemoteCache.asInstanceOf[RemoteCache[Int, Runner]]

      rdd.writeToInfinispan(getConfiguration)

      cache.size() shouldBe 1000
      cache.get(350L).getName shouldBe "name350"
   }
}

@DoNotDiscover
class WriteSecureSuite extends WriteSuite with SingleSecureServer
