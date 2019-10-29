package org.infinispan.spark.test

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.scalatest.{Matchers, Suite}

import scala.collection.JavaConverters._

trait DatasetAssertions[E] {
   this: Suite with RemoteTest with Matchers =>

   def assertRows(a: Array[Row], p: E => Boolean): Unit = row2String(a) shouldBe getFromCache(p)

   def assertDataset(ds: Dataset[Row], p: E => Boolean): Unit = assertRows(ds.collect(), p)

   def assertSql(session: SparkSession, sql: String, p: E => Boolean): Unit = row2String(session.sql(sql).collect()) shouldBe getFromCache(p)

   def row2String(rows: Array[Row]): Set[String] = rows.map(_.getAs[String]("name")).toSet

   def row2String(e: E): String

   def getFromCache(p: E => Boolean): Set[String] = {
      (getRemoteCache.retrieveEntries(null, 100).asScala)
        .map(_.getValue.asInstanceOf[E]).withFilter(p).map(row2String).toSet
   }

}
