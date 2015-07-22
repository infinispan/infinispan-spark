package org.infinispan.spark.test

import java.util.Properties

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

   private lazy val config: SparkConf = new SparkConf().setMaster("local[8]").setAppName(this.getClass.getName)
   protected var sc: SparkContext = _

   def createInfinispanRDD[K, V] = {
      new InfinispanRDD[K, V](sc, configuration = getConfiguration)
   }

   override protected def beforeAll(): Unit = {
      sc = new SparkContext(config)
      super.beforeAll()
   }

   override protected def afterAll(): Unit = {
      sc.stop()
      super.afterAll()
   }

}
