package org.infinispan.spark.test

import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
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
           .set("spark.driver.host","127.0.0.1")

   protected var jsc: JavaSparkContext = _

   override protected def beforeAll(): Unit = {
      jsc = new JavaSparkContext(config)
      super.beforeAll()
   }

   override protected def afterAll(): Unit = {
      jsc.stop()
      super.afterAll()
   }
}
