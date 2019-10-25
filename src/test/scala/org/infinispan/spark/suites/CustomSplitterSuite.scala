package org.infinispan.spark.suites

import org.infinispan.spark.rdd.InfinispanRDD
import org.infinispan.spark.test._
import org.scalatest.{DoNotDiscover, FunSuite, Matchers}


@DoNotDiscover
class CustomSplitterSuite extends FunSuite with WordCache with Spark with MultipleServers with Matchers {

   override protected def getNumEntries: Int = 100

   test("RDD with custom splitter implementation") {
      val customSplitter = new FixedPartitionsSplitter(2)

      val rdd = new InfinispanRDD[Int, String](sc, getConfiguration, customSplitter)

      rdd.values.count() shouldBe getNumEntries
      customSplitter.lastSplitCount shouldBe 2
   }
}
