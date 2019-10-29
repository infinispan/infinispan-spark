package org.infinispan.spark.suites

import org.infinispan.spark.test.{Spark, WordCache}
import org.scalatest.{FunSuite, Matchers}

abstract class RDDRetrievalTest extends FunSuite with Matchers {
   self: WordCache with Spark =>

   test("RDD Operators") {
      val infinispanRDD = createInfinispanRDD[Int, String]

      // Count
      infinispanRDD.count() shouldBe getNumEntries

      // Sort By Key
      val first = infinispanRDD.sortByKey().first()
      first._1 shouldBe 1

      // Count by Key
      val map = infinispanRDD.countByKey()
      map.forall { case (_, v) => v == 1 } shouldBe true

      // Filter
      val filteredRDD = infinispanRDD.filter { case (_, v) => v.startsWith("a") }
      filteredRDD.values.collect().forall(_.startsWith("a")) shouldBe true

      // Collect and Sort
      val arrayOfTuple = infinispanRDD.collect().sorted
      arrayOfTuple.length shouldBe getNumEntries
      arrayOfTuple.head shouldBe((1, wordsCache.get(1)))

      // Max/Min
      val maxEntry = infinispanRDD.max()
      val minEntry = infinispanRDD.min()
      minEntry shouldBe((1, wordsCache get 1))
      maxEntry shouldBe((getNumEntries, wordsCache get getNumEntries))

      // RDD combination operations
      val data = Array(1, 2, 3, 4, 5)
      val aRDD = sc.parallelize(data)

      val cartesianRDD = aRDD.cartesian(infinispanRDD)
      cartesianRDD.count shouldBe getNumEntries * data.length

      val first5 = (1 to 5).map(i => (i, wordsCache.get(i)))
      val otherRDD = sc.makeRDD(first5)
      val subtractedRDD = infinispanRDD.subtract(otherRDD, numPartitions = 2)
      subtractedRDD.count shouldBe (getNumEntries - otherRDD.count)

      // Word count map reduce
      val resultRDD = infinispanRDD.map { case (_, v) => v }.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
      val firstWordCount = resultRDD.first()
      val count = infinispanRDD.values.flatMap(_.split(" ")).countByValue().get(firstWordCount._1).get
      count shouldBe firstWordCount._2

   }
}
