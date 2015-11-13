package org.infinispan.spark.suites

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.infinispan.spark.domain.Runner
import org.infinispan.spark.test._
import org.scalatest.{DoNotDiscover, FunSuite, Matchers}

@DoNotDiscover
class SQLSuite extends FunSuite with RunnersCache with Spark with MultipleServers with Matchers {

   override def getNumEntries: Int = 100

   test("SQL Group By") {
      withSqlContext { (sqlContext, runnersRDD) =>
         val winners = sqlContext.sql(
            """
              |SELECT MIN(r.finishTimeSeconds) as time, first(r.name) as name, first(r.age) as age
              |FROM runners r WHERE
              |r.finished = true GROUP BY r.age
              |
            """.stripMargin).collect()

         /* Check winners */
         winners.foreach { row =>
            val winnerTime = row.getAs[Int]("time")
            val age = row.getAs[Int]("age")
            val fasterOfAge = runnersRDD.filter(r => r.getAge == age && r.getFinished).sortBy(_.getFinishTimeSeconds).first()
            fasterOfAge.getFinishTimeSeconds shouldBe winnerTime
         }
      }
   }

   test("SQL Count") {
      withSqlContext { (sqlContext, _) =>
         val count = sqlContext.sql("SELECT count(*) AS result from runners").collect().head.getAs[Long]("result")
         count shouldBe getNumEntries
      }
   }

   private def withSqlContext(f: (SQLContext, RDD[Runner]) => Any) = {
      val runnersRDD = createInfinispanRDD[Integer, Runner].values
      val sqlContext = new SQLContext(sc)
      val dataFrame = sqlContext.createDataFrame(runnersRDD, classOf[Runner])
      dataFrame.registerTempTable("runners")
      f(sqlContext, runnersRDD)
   }

   override def getCacheType: CacheType.Value = CacheType.REPLICATED
}
