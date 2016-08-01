package org.infinispan.spark.suites

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.infinispan.spark.domain.Runner
import org.infinispan.spark.test._
import org.scalatest.{DoNotDiscover, FunSuite, Matchers}

@DoNotDiscover
class SQLSuite extends FunSuite with RunnersCache with Spark with MultipleServers with Matchers {

   override def getNumEntries: Int = 100

   test("SQL Group By") {
      withSession { (session, runnersRDD) =>
         val winners = session.sql(
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
      withSession { (session, _) =>
         val count = session.sql("SELECT count(*) AS result from runners").collect().head.getAs[Long]("result")
         count shouldBe getNumEntries
      }
   }



   private def withSession(f: (SparkSession, RDD[Runner]) => Any) = {
      val runnersRDD = createInfinispanRDD[Integer, Runner].values
      val session = SparkSession.builder().config(getSparkConfig).getOrCreate()
      val dataFrame = session.createDataFrame(runnersRDD, classOf[Runner])
      dataFrame.createOrReplaceTempView("runners")
      f(session, runnersRDD)
   }

   override def getCacheType: CacheType.Value = CacheType.REPLICATED
}
