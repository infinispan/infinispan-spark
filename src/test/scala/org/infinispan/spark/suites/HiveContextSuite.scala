package org.infinispan.spark.suites

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.infinispan.spark.domain.Runner
import org.infinispan.spark.test.{RunnersCache, SingleServer, Spark}
import org.scalatest.{DoNotDiscover, FunSuite, Matchers}

@DoNotDiscover
class HiveContextSuite extends FunSuite with RunnersCache with Spark with SingleServer with Matchers {
   override protected def getNumEntries: Int = 200

   test("Hive SQL") {
      withHiveContext { (hiveContext: HiveContext, runnersRDD) =>
         val sample = hiveContext.sql(
            """
             SELECT * FROM runners TABLESAMPLE(10 ROWS) s
            """.stripMargin).collect()

         sample.length shouldBe 10
      }
   }

   private def withHiveContext(f: (HiveContext, RDD[Runner]) => Any) = {
      val runnersRDD = createInfinispanRDD[Integer, Runner].values
      val hiveContext = new HiveContext(sc)
      val dataFrame = hiveContext.createDataFrame(runnersRDD)
      dataFrame.registerTempTable("runners")
      f(hiveContext, runnersRDD)
   }

}
