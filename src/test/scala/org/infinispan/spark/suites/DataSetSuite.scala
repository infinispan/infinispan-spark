package org.infinispan.spark.suites

import org.apache.spark.sql.SparkSession
import org.infinispan.spark.domain.Runner
import org.infinispan.spark.test._
import org.scalatest.{DoNotDiscover, FunSuite, Matchers}

@DoNotDiscover
class DataSetSuite extends FunSuite with RunnersCache with Spark with MultipleServers with Matchers
  with DatasetAssertions[Runner] {

   override protected def getNumEntries: Int = 100

   override def getConfiguration = {
      val config = super.getConfiguration
      config.addProtoAnnotatedClass(classOf[Runner])
      config.setAutoRegisterProto()
      config
   }

   test("read data using the DataFrame API") {
      val config = getConfiguration.toStringsMap
      val df = getSparkSession.read.format("infinispan").options(config).load()

      val filter = df.filter(df("age").gt(30)).filter(df("age").lt(40))

      assertDataset(filter, r => r.getAge > 30 && r.getAge < 40)
   }

   test("read using SQL single filter") {
      val config = getConfiguration.toStringsMap
      val session = getSparkSession

      val df = getSparkSession.read.format("infinispan").options(config).load()

      df.createOrReplaceTempView("runner")

      assertSql(session, "From runner where age > 30", _.getAge > 30)
      assertSql(session, "From runner where age >= 30", _.getAge >= 30)
      assertSql(session, "From runner where age < 50", _.getAge < 50)
      assertSql(session, "From runner where age <= 50", _.getAge <= 50)
      assertSql(session, "From runner where name LIKE 'runner1%'", _.getName.startsWith("runner1"))
      assertSql(session, "From runner where name LIKE '%unner2%'", _.getName.contains("unner2"))
   }

   test("read using SQL and projections") {
      val config = getConfiguration.toStringsMap
      val session: SparkSession = getSparkSession
      val df = session.read.format("infinispan").options(config).load()
      df.createOrReplaceTempView("runner")

      val rows = getSparkSession.sql("select name, finished from runner where age > 20").collect()

      assertRows(rows, _.getAge > 20)

      val firstRow = rows(0)

      firstRow.length shouldBe 2
      firstRow.get(0).getClass shouldBe classOf[String]
      firstRow.get(1).getClass shouldBe classOf[java.lang.Boolean]
   }

   test("read using SQL and combined predicates") {
      val config = getConfiguration.toStringsMap
      implicit val session = getSparkSession
      val df = session.read.format("infinispan").options(config).load()
      df.createOrReplaceTempView("runner")

      assertSql(session, "select * from runner where finished = false and (age > 30 or age < 50)", r => !r.getFinished && (r.getAge > 30 || r.getAge < 50))
   }

   override def row2String(e: Runner): String = e.getName
}
