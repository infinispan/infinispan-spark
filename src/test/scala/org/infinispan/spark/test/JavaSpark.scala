package org.infinispan.spark.test

import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.SparkSession
import org.infinispan.spark.serializer._
import org.scalatest.{BeforeAndAfterAll, Suite}

/**
  * Trait to be mixed-in by tests that require a JavaSparkContext
  *
  * @author gustavonalle
  */
trait JavaSpark extends BeforeAndAfterAll {
   this: Suite with RemoteTest =>

   private lazy val config: SparkConf = new SparkConf().setMaster("local[4]")
     .setAppName(this.getClass.getName)
     .set("spark.serializer", classOf[JBossMarshallingSerializer].getName)
     .set("spark.driver.host", "127.0.0.1")

   protected var sparkSession: SparkSession = _
   protected var jsc: JavaSparkContext = _

   override protected def beforeAll(): Unit = {
      sparkSession = SparkSession.builder().config(config).getOrCreate()
      jsc = new JavaSparkContext(sparkSession.sparkContext)
      super.beforeAll()
   }

   override protected def afterAll(): Unit = {
      jsc.stop()
      sparkSession.stop()
      sparkSession.stop()
      super.afterAll()
   }
}
