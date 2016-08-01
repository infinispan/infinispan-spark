package org.infinispan.spark.test

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.infinispan.spark.rdd.InfinispanRDD
import org.scalatest.{BeforeAndAfterAll, Suite}

/**
 * Trait to be mixed-in by tests that require a org.apache.spark.SparkContext
 *
 * @author gustavonalle
 */
trait Spark extends BeforeAndAfterAll {
   this: Suite with RemoteTest =>

   protected def getSparkConfig = new SparkConf().setMaster("local[4]").setAppName(this.getClass.getName).set("spark.driver.host","127.0.0.1")

   protected var sparkSession: SparkSession = _
   protected var sc: SparkContext = _

   def createInfinispanRDD[K, V] = {
      new InfinispanRDD[K, V](sc, configuration = getConfiguration)
   }

   override protected def beforeAll(): Unit = {
      sparkSession = SparkSession.builder().config(getSparkConfig).getOrCreate()
      sc = sparkSession.sparkContext
      super.beforeAll()
   }

   override protected def afterAll(): Unit = {
      sparkSession.stop()
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
      super.afterAll()
   }

}
