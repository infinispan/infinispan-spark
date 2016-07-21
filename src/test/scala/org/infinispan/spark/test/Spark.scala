package org.infinispan.spark.test

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

   protected def getSparkConfig = new SparkConf().setMaster("local[8]").setAppName(this.getClass.getName).set("spark.driver.host","127.0.0.1")

   protected var sc: SparkContext = _

   def createInfinispanRDD[K, V] = {
      new InfinispanRDD[K, V](sc, configuration = getConfiguration)
   }

   override protected def beforeAll(): Unit = {
      sc = new SparkContext(getSparkConfig)
      super.beforeAll()
   }

   override protected def afterAll(): Unit = {
      sc.stop()
      super.afterAll()
   }

}
